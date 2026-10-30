package com.example.loadTestBilla.game;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;
import com.example.loadTestBilla.game.MatchStore.Accepted;
import com.example.loadTestBilla.game.MatchStore.Expiry;
import com.example.loadTestBilla.game.MatchStore.Participant;
import com.example.loadTestBilla.game.MatchStore.Ready;
import com.example.loadTestBilla.game.MatchStore.Rejected;
import com.example.loadTestBilla.game.MatchStore.RepOutcome;
import com.example.loadTestBilla.game.MatchStore.Result;
import com.example.loadTestBilla.realtime.Frames;
import com.example.loadTestBilla.realtime.SessionRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


@Service
public class MatchService {

	private static final Logger log = LoggerFactory.getLogger(MatchService.class);


	private static final Duration STUCK_AFTER = Duration.ofSeconds(60);

	private final MatchStore store;

	private final MatchPersistence persistence;

	private final SessionRegistry sessions;

	private final TaskScheduler scheduler;

	private final GameProperties properties;

	private final MeterRegistry meters;

	private final Counter opened;

	private final Counter persistFailures;

	private final Counter recovered;

	public MatchService(MatchStore store, MatchPersistence persistence, SessionRegistry sessions,
			TaskScheduler scheduler, GameProperties properties, MeterRegistry meters) {
		this.store = store;
		this.persistence = persistence;
		this.sessions = sessions;
		this.scheduler = scheduler;
		this.properties = properties;
		this.meters = meters;
		// Not "game.matches.created": Prometheus treats a "_created" suffix as reserved and renames the meter.
		this.opened = meters.counter("game.matches.opened");
		this.persistFailures = meters.counter("game.matches.persist.failures");
		this.recovered = meters.counter("game.matches.recovered");
		// Cluster-wide: every instance reports the same number, read from Redis on each scrape.
		Gauge.builder("game.matches.live", store, MatchStore::liveCount).register(meters);
	}

	public boolean isInMatch(UUID playerId) {
		Trace.call(Area.MATCH, "MatchService.isInMatch");
		return store.isInMatch(playerId);
	}


	boolean create(Participant a, Participant b) {
		Trace.call(Area.MATCH, "MatchService.create", a.name() + " vs " + b.name());
		UUID matchId = UUID.randomUUID();
		if (!store.create(matchId, a, b)) {
			return false;
		}
		try {
			persistence.created(matchId, a.id(), b.id(), Instant.now());
		}
		catch (RuntimeException ex) {
			log.warn("Could not save new match {}", matchId, ex);
			persistFailures.increment();
			store.release(matchId, a.id(), b.id());
			return false;
		}
		opened.increment();
		sessions.send(a.id(), matchFound(matchId, b));
		sessions.send(b.id(), matchFound(matchId, a));
		return true;
	}

	public void ready(UUID playerId, UUID matchId, Object frameId) {
		Trace.call(Area.MATCH, "MatchService.ready");
		Ready ready = store.ready(matchId, playerId);
		if (ready == Ready.NOT_IN_MATCH) {
			notInMatch(playerId, frameId);
			return;
		}
		sessions.send(playerId, Frames.of("READY_ACK", "id", frameId, "matchId", matchId));
		if (ready == Ready.BOTH) {
			scheduler.schedule(() -> begin(matchId), Instant.now().plus(properties.countdown()));
		}
	}

	public void rep(UUID playerId, UUID matchId, Object frameId) {
		Trace.call(Area.MATCH, "MatchService.rep");
		RepOutcome outcome = store.rep(matchId, playerId, properties.minRepInterval().toMillis(),
				properties.targetReps());
		if (outcome == null) {
			notInMatch(playerId, frameId);
			return;
		}
		if (outcome instanceof Rejected rejected) {
			meters.counter("game.reps", "outcome", "rejected", "reason", rejected.reason()).increment();
			sessions.send(playerId, Frames.of("REP_REJECTED", "id", frameId, "reason", rejected.reason()));
			return;
		}
		Accepted accepted = (Accepted) outcome;
		meters.counter("game.reps", "outcome", "accepted", "reason", "none").increment();
		sessions.send(playerId, Frames.of("REP_ACCEPTED", "id", frameId, "count", accepted.count()));
		sessions.send(accepted.opponentId(), Frames.of("OPPONENT_REP", "matchId", matchId, "count", accepted.count()));
		if (accepted.finished() != null) {
			complete(accepted.finished());
		}
	}


	public void leave(UUID playerId) {
		Trace.call(Area.MATCH, "MatchService.leave");
		UUID matchId = store.matchOf(playerId);
		if (matchId == null) {
			return;
		}
		Result result = store.forfeit(matchId, playerId);
		if (result != null) {
			complete(result);
		}
	}


	@Scheduled(fixedDelay = 5000)
	void sweep() {
		try {
			long soonestDue = Math.min(properties.readyTimeout().toMillis(), properties.maxMatchDuration().toMillis());
			for (String id : store.createdBefore(System.currentTimeMillis() - soonestDue)) {
				UUID matchId = UUID.fromString(id);
				Trace.call(Area.MATCH, "MatchService.sweep", matchId);
				Expiry expiry = store.expire(matchId, properties.readyTimeout().toMillis(),
						properties.countdown().toMillis(), properties.maxMatchDuration().toMillis(),
						STUCK_AFTER.toMillis());
				if (expiry.gone()) {
					store.forget(matchId);
				}
				else if (expiry.result() != null) {
					if (expiry.stuck()) {
						recovered.increment();
						log.warn("Recovering match {} that an instance finished but never released", matchId);
					}
					complete(expiry.result());
				}
			}
		}
		catch (RuntimeException ex) {
			log.warn("Sweep failed; the next one will retry", ex);
		}
	}

	private void begin(UUID matchId) {
		Trace.call(Area.MATCH, "MatchService.begin", "countdown over");
		try {
			List<UUID> players = store.start(matchId);
			if (players == null) {
				return;
			}
			persistence.started(matchId, Instant.now());
			Map<String, Object> frame = Frames.of("MATCH_STARTED", "matchId", matchId, "target",
					properties.targetReps());
			sessions.send(players.get(0), frame);
			sessions.send(players.get(1), frame);
		}
		catch (RuntimeException ex) {
			log.warn("Could not start match {}", matchId, ex);
			persistFailures.increment();
		}
	}


	private void complete(Result result) {
		Trace.call(Area.MATCH, "MatchService.complete", result.reason());
		try {
			persistence.finished(result, Instant.now());
		}
		catch (RuntimeException ex) {
			log.warn("Could not save the result of match {}", result.matchId(), ex);
			persistFailures.increment();
		}
		store.release(result.matchId(), result.player1().id(), result.player2().id());
		meters.counter("game.matches.finished", "reason", result.reason()).increment();
		sendFinished(result, result.player1(), result.player1Reps(), result.player2Reps());
		sendFinished(result, result.player2(), result.player2Reps(), result.player1Reps());
	}

	private void sendFinished(Result result, Participant to, int yourReps, int opponentReps) {
		sessions.send(to.id(), Frames.of("MATCH_FINISHED", "matchId", result.matchId(), "reason", result.reason(),
				"winnerId", result.winnerId(), "yourReps", yourReps, "opponentReps", opponentReps, "xpEarned",
				persistence.xpFor(result, to.id())));
	}

	private Map<String, Object> matchFound(UUID matchId, Participant opponent) {
		return Frames.of("MATCH_FOUND", "matchId", matchId, "opponent",
				Map.of("id", opponent.id(), "name", opponent.name()), "target", properties.targetReps());
	}

	private void notInMatch(UUID playerId, Object frameId) {
		sessions.send(playerId, Frames.of("ERROR", "id", frameId, "code", "not-in-match"));
	}

}
