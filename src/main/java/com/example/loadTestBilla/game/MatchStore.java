package com.example.loadTestBilla.game;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.example.loadTestBilla.common.RedisScripts;
import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;

import org.springframework.stereotype.Component;


@Component
class MatchStore {

	record Participant(UUID id, String name) {
	}


	record Result(UUID matchId, Participant player1, Participant player2, int player1Reps, int player2Reps,
			UUID winnerId, String reason) {

		boolean cancelled() {
			return "CANCELLED".equals(reason);
		}

	}

	sealed interface RepOutcome {
	}


	record Accepted(int count, UUID opponentId, Result finished) implements RepOutcome {
	}

	record Rejected(String reason) implements RepOutcome {
	}

	enum Ready {
		NOT_IN_MATCH, IGNORED, WAITING, BOTH
	}

	static final String LIVE = "matches:live";


	private static final String COMMON = """
			local function nowms()
			  local t = redis.call('TIME')
			  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
			end
			local function fmt(n) return string.format('%.0f', n) end
			local function describe(m)
			  local f = redis.call('HMGET', m, 'reason', 'winner', 'p1', 'n1', 'p2', 'n2', 'reps1', 'reps2')
			  return {'FINISHED', f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]}
			end
			local function finish(m, winner, reason)
			  redis.call('HSET', m, 'state', 'FINISHED', 'winner', winner, 'reason', reason, 'finished', fmt(nowms()))
			  return describe(m)
			end
			""";

	private static final String CREATE = COMMON + """
			if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 then return {'BUSY'} end
			local now = fmt(nowms())
			redis.call('HSET', KEYS[1], 'state', 'WAITING_READY', 'p1', ARGV[2], 'n1', ARGV[3], 'p2', ARGV[4],
			  'n2', ARGV[5], 'ready1', '0', 'ready2', '0', 'reps1', '0', 'reps2', '0', 'last1', '0', 'last2', '0',
			  'created', now, 'started', '0')
			redis.call('SET', KEYS[2], ARGV[1])
			redis.call('SET', KEYS[3], ARGV[1])
			redis.call('ZADD', KEYS[4], now, ARGV[1])
			return {'OK'}
			""";

	private static final String READY = """
			local f = redis.call('HMGET', KEYS[1], 'state', 'p1', 'p2')
			local idx
			if f[2] == ARGV[1] then idx = '1' elseif f[3] == ARGV[1] then idx = '2' else return {'NOT_IN_MATCH'} end
			if f[1] ~= 'WAITING_READY' then return {'IGNORED'} end
			redis.call('HSET', KEYS[1], 'ready' .. idx, '1')
			local r = redis.call('HMGET', KEYS[1], 'ready1', 'ready2')
			if r[1] == '1' and r[2] == '1' then
			  redis.call('HSET', KEYS[1], 'state', 'COUNTDOWN')
			  return {'BOTH'}
			end
			return {'WAITING'}
			""";

	private static final String START = COMMON + """
			local f = redis.call('HMGET', KEYS[1], 'state', 'p1', 'p2')
			if f[1] ~= 'COUNTDOWN' then return {} end
			redis.call('HSET', KEYS[1], 'state', 'IN_PROGRESS', 'started', fmt(nowms()))
			return {'STARTED', f[2], f[3]}
			""";

	private static final String REP = COMMON + """
			local f = redis.call('HMGET', KEYS[1], 'state', 'p1', 'p2')
			local idx, opp
			if f[2] == ARGV[1] then idx, opp = '1', f[3] elseif f[3] == ARGV[1] then idx, opp = '2', f[2]
			else return {'NOT_IN_MATCH'} end
			if f[1] ~= 'IN_PROGRESS' then return {'REJECTED', 'match-not-in-progress'} end
			local now = nowms()
			local last = tonumber(redis.call('HGET', KEYS[1], 'last' .. idx))
			if last > 0 and now - last < tonumber(ARGV[2]) then return {'REJECTED', 'too-fast'} end
			redis.call('HSET', KEYS[1], 'last' .. idx, fmt(now))
			local count = redis.call('HINCRBY', KEYS[1], 'reps' .. idx, 1)
			local out = {'ACCEPTED', tostring(count), opp}
			if count >= tonumber(ARGV[3]) then
			  for _, v in ipairs(finish(KEYS[1], ARGV[1], 'TARGET_REACHED')) do table.insert(out, v) end
			end
			return out
			""";

	private static final String FORFEIT = COMMON + """
			local f = redis.call('HMGET', KEYS[1], 'state', 'p1', 'p2')
			if f[2] ~= ARGV[1] and f[3] ~= ARGV[1] then return {} end
			if f[1] == 'FINISHED' then return {} end
			if f[1] ~= 'IN_PROGRESS' then return finish(KEYS[1], '', 'CANCELLED') end
			if f[2] == ARGV[1] then return finish(KEYS[1], f[3], 'FORFEIT') end
			return finish(KEYS[1], f[2], 'FORFEIT')
			""";


	private static final String EXPIRE = COMMON + """
			if redis.call('EXISTS', KEYS[1]) == 0 then return {'GONE'} end
			local f = redis.call('HMGET', KEYS[1], 'state', 'created', 'started', 'reps1', 'reps2', 'p1', 'p2', 'finished')
			local now = nowms()
			local created = tonumber(f[2])
			if f[1] == 'WAITING_READY' and now > created + tonumber(ARGV[1]) then
			  return finish(KEYS[1], '', 'CANCELLED') end
			if f[1] == 'COUNTDOWN' and now > created + tonumber(ARGV[1]) + tonumber(ARGV[2]) + 10000 then
			  return finish(KEYS[1], '', 'CANCELLED') end
			if f[1] == 'IN_PROGRESS' and now > tonumber(f[3]) + tonumber(ARGV[3]) then
			  local r1, r2 = tonumber(f[4]), tonumber(f[5])
			  local winner = ''
			  if r1 > r2 then winner = f[6] elseif r2 > r1 then winner = f[7] end
			  return finish(KEYS[1], winner, 'TIMEOUT') end
			if f[1] == 'FINISHED' and now > tonumber(f[8]) + tonumber(ARGV[4]) then
			  local d = describe(KEYS[1]); d[1] = 'STUCK'; return d end
			return {}
			""";

	private static final String RELEASE = """
			if redis.call('GET', KEYS[2]) == ARGV[1] then redis.call('DEL', KEYS[2]) end
			if redis.call('GET', KEYS[3]) == ARGV[1] then redis.call('DEL', KEYS[3]) end
			redis.call('ZREM', KEYS[4], ARGV[1])
			redis.call('DEL', KEYS[1])
			return {}
			""";

	private final RedisScripts scripts;

	MatchStore(RedisScripts scripts) {
		this.scripts = scripts;
	}

	static String matchKey(UUID matchId) {
		return "match:" + matchId;
	}

	static String playerKey(UUID playerId) {
		return "player:" + playerId + ":match";
	}


	boolean create(UUID matchId, Participant a, Participant b) {
		Trace.call(Area.REDIS, "MatchStore.create");
		List<String> r = scripts.run("create", CREATE,
				List.of(matchKey(matchId), playerKey(a.id()), playerKey(b.id()), LIVE), matchId, a.id(), a.name(),
				b.id(), b.name());
		return "OK".equals(first(r));
	}

	Ready ready(UUID matchId, UUID playerId) {
		Trace.call(Area.REDIS, "MatchStore.ready");
		return Ready.valueOf(first(scripts.run("ready", READY, List.of(matchKey(matchId)), playerId)));
	}

	List<UUID> start(UUID matchId) {
		Trace.call(Area.REDIS, "MatchStore.start");
		List<String> r = scripts.run("start", START, List.of(matchKey(matchId)));
		return "STARTED".equals(first(r)) ? List.of(UUID.fromString(r.get(1)), UUID.fromString(r.get(2))) : null;
	}


	RepOutcome rep(UUID matchId, UUID playerId, long minIntervalMs, int target) {
		Trace.call(Area.REDIS, "MatchStore.rep");
		List<String> r = scripts.run("rep", REP, List.of(matchKey(matchId)), playerId, minIntervalMs, target);
		return switch (first(r)) {
			case "REJECTED" -> new Rejected(r.get(1));
			case "ACCEPTED" -> new Accepted(Integer.parseInt(r.get(1)), UUID.fromString(r.get(2)),
					(r.size() > 3) ? result(matchId, r, 3) : null);
			default -> null;
		};
	}


	Result forfeit(UUID matchId, UUID leaverId) {
		Trace.call(Area.REDIS, "MatchStore.forfeit");
		List<String> r = scripts.run("forfeit", FORFEIT, List.of(matchKey(matchId)), leaverId);
		return "FINISHED".equals(first(r)) ? result(matchId, r, 0) : null;
	}


	record Expiry(boolean gone, boolean stuck, Result result) {
	}

	Expiry expire(UUID matchId, long readyTimeoutMs, long countdownMs, long maxDurationMs, long stuckAfterMs) {
		Trace.call(Area.REDIS, "MatchStore.expire");
		List<String> r = scripts.run("expire", EXPIRE, List.of(matchKey(matchId)), readyTimeoutMs, countdownMs,
				maxDurationMs, stuckAfterMs);
		return switch (first(r)) {
			case "GONE" -> new Expiry(true, false, null);
			case "FINISHED" -> new Expiry(false, false, result(matchId, r, 0));
			case "STUCK" -> new Expiry(false, true, result(matchId, r, 0));
			default -> new Expiry(false, false, null);
		};
	}

	void release(UUID matchId, UUID player1, UUID player2) {
		Trace.call(Area.REDIS, "MatchStore.release");
		scripts.run("release", RELEASE, List.of(matchKey(matchId), playerKey(player1), playerKey(player2), LIVE),
				matchId);
	}

	void forget(UUID matchId) {
		Trace.call(Area.REDIS, "MatchStore.forget");
		scripts.template().opsForZSet().remove(LIVE, matchId.toString());
	}

	UUID matchOf(UUID playerId) {
		Trace.call(Area.REDIS, "MatchStore.matchOf");
		String id = scripts.template().opsForValue().get(playerKey(playerId));
		return (id != null) ? UUID.fromString(id) : null;
	}

	boolean isInMatch(UUID playerId) {
		Trace.call(Area.REDIS, "MatchStore.isInMatch");
		return Boolean.TRUE.equals(scripts.template().hasKey(playerKey(playerId)));
	}

	Set<String> createdBefore(long cutoffMs) {
		Set<String> ids = scripts.template().opsForZSet().rangeByScore(LIVE, 0, cutoffMs);
		return (ids != null) ? ids : Set.of();
	}

	long liveCount() {
		Long n = scripts.template().opsForZSet().zCard(LIVE);
		return (n != null) ? n : 0;
	}

	private static Result result(UUID matchId, List<String> r, int at) {
		String winner = r.get(at + 2);
		return new Result(matchId, new Participant(UUID.fromString(r.get(at + 3)), r.get(at + 4)),
				new Participant(UUID.fromString(r.get(at + 5)), r.get(at + 6)), Integer.parseInt(r.get(at + 7)),
				Integer.parseInt(r.get(at + 8)), (winner == null || winner.isEmpty()) ? null : UUID.fromString(winner),
				r.get(at + 1));
	}

	private static String first(List<String> r) {
		return r.isEmpty() ? "" : r.get(0);
	}

}
