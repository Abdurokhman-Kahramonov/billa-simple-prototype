package com.example.loadTestBilla.game;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.example.loadTestBilla.common.ApiException;
import com.example.loadTestBilla.common.RedisScripts;
import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;
import com.example.loadTestBilla.game.MatchStore.Participant;
import com.example.loadTestBilla.realtime.SessionRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class MatchmakingService {

	private static final Logger log = LoggerFactory.getLogger(MatchmakingService.class);

	private static final List<String> QUEUE_KEYS = List.of("queue", "queue:names", "queue:since");

	private static final long PAIRING_TTL_MS = 10_000;

	private static final String COMMON = """
			local function nowms()
			  local t = redis.call('TIME')
			  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
			end
			""";

	private static final String ENQUEUE = COMMON + """
			if redis.call('EXISTS', KEYS[4]) == 1 then return {'already-in-match'} end
			if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 or redis.call('EXISTS', KEYS[5]) == 1 then
			  return {'already-queued'} end
			redis.call('RPUSH', KEYS[1], ARGV[1])
			redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
			redis.call('HSET', KEYS[3], ARGV[1], string.format('%.0f', nowms()))
			return {'OK'}
			""";

	private static final String POP_TWO = COMMON + """
			if redis.call('LLEN', KEYS[1]) < 2 then return {} end
			local out = {string.format('%.0f', nowms())}
			for i = 1, 2 do
			  local id = redis.call('LPOP', KEYS[1])
			  table.insert(out, id)
			  table.insert(out, redis.call('HGET', KEYS[2], id) or '')
			  table.insert(out, redis.call('HGET', KEYS[3], id) or '0')
			  redis.call('HDEL', KEYS[2], id)
			  redis.call('HDEL', KEYS[3], id)
			  redis.call('SET', 'pairing:' .. id, '1', 'PX', ARGV[1])
			end
			return out
			""";


	private static final String REQUEUE = """
			redis.call('DEL', 'pairing:' .. ARGV[1])
			if redis.call('EXISTS', 'player:' .. ARGV[1] .. ':match') == 1 then return {} end
			redis.call('LPUSH', KEYS[1], ARGV[1])
			redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
			redis.call('HSET', KEYS[3], ARGV[1], ARGV[3])
			return {}
			""";


	private static final String LEAVE = """
			if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 0 then return {} end
			redis.call('LREM', KEYS[1], 0, ARGV[1])
			redis.call('HDEL', KEYS[2], ARGV[1])
			redis.call('HDEL', KEYS[3], ARGV[1])
			return {'1'}
			""";

	private record Popped(Participant player, long since) {
	}

	private final RedisScripts scripts;

	private final MatchService matches;

	private final SessionRegistry sessions;

	private final Timer queueWait;

	public MatchmakingService(RedisScripts scripts, MatchService matches, SessionRegistry sessions,
			MeterRegistry meters) {
		this.scripts = scripts;
		this.matches = matches;
		this.sessions = sessions;
		this.queueWait = Timer.builder("game.queue.wait").description("Time from POST /api/queue to MATCH_FOUND")
			.register(meters);
		Gauge.builder("game.queue.size", this, MatchmakingService::size).register(meters);
	}

	public void enqueue(UUID playerId, String name) {
		Trace.call(Area.MATCHMAKING, "MatchmakingService.enqueue", name);
		if (!sessions.isConnected(playerId)) {
			throw new ApiException(HttpStatus.CONFLICT, "not-connected");
		}
		List<String> r = scripts.run("enqueue", ENQUEUE, List.of("queue", "queue:names", "queue:since",
				MatchStore.playerKey(playerId), "pairing:" + playerId), playerId, name);
		if (!"OK".equals(r.get(0))) {
			throw new ApiException(HttpStatus.CONFLICT, r.get(0));
		}
	}


	public boolean leave(UUID playerId) {
		Trace.call(Area.MATCHMAKING, "MatchmakingService.leave");
		return !scripts.run("leave", LEAVE, QUEUE_KEYS, playerId).isEmpty();
	}

	@Scheduled(fixedDelayString = "${game.matchmaking-interval}")
	void pairUp() {
		try {
			List<String> r;
			while (!(r = scripts.run("pop-two", POP_TWO, QUEUE_KEYS, PAIRING_TTL_MS)).isEmpty()) {
				Trace.call(Area.MATCHMAKING, "MatchmakingService.pairUp", "popped two players");
				long now = Long.parseLong(r.get(0));
				Popped a = popped(r, 1);
				Popped b = popped(r, 4);
				pair(a, b, now);
			}
		}
		catch (RuntimeException ex) {
			log.warn("Pairing failed; the next pass will retry", ex);
		}
	}

	private void pair(Popped a, Popped b, long now) {
		Trace.call(Area.MATCHMAKING, "MatchmakingService.pair", a.player().name() + " vs " + b.player().name());
		UUID aId = a.player().id();
		UUID bId = b.player().id();
		boolean aHere = sessions.isConnected(aId);
		boolean bHere = sessions.isConnected(bId);
		if (aHere && bHere && matches.create(a.player(), b.player())) {
			queueWait.record(Duration.ofMillis(now - a.since()));
			queueWait.record(Duration.ofMillis(now - b.since()));
			scripts.template().delete(List.of("pairing:" + aId, "pairing:" + bId));
			return;
		}
		for (Popped p : List.of(a, b)) {
			if (sessions.isConnected(p.player().id())) {
				Trace.call(Area.MATCHMAKING, "MatchmakingService.pair", "back to the queue: " + p.player().name());
				scripts.run("requeue", REQUEUE, QUEUE_KEYS, p.player().id(), p.player().name(), p.since());
			}
			else {
				scripts.template().delete("pairing:" + p.player().id());
			}
		}
	}

	private static Popped popped(List<String> r, int at) {
		return new Popped(new Participant(UUID.fromString(r.get(at)), r.get(at + 1)), Long.parseLong(r.get(at + 2)));
	}

	private long size() {
		Long n = scripts.template().opsForList().size("queue");
		return (n != null) ? n : 0;
	}

	Map<String, Object> status(UUID playerId) {
		Trace.call(Area.MATCHMAKING, "MatchmakingService.status");
		boolean queued = scripts.template().opsForHash().hasKey("queue:names", playerId.toString());
		return Map.of("queued", queued, "inMatch", matches.isInMatch(playerId));
	}

}
