package com.example.loadTestBilla.game;

import java.time.Instant;
import java.util.UUID;

import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;
import com.example.loadTestBilla.game.MatchStore.Result;
import com.example.loadTestBilla.player.XpTransaction;
import com.example.loadTestBilla.player.XpTransactionRepo;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;


@Component
class MatchPersistence {

	private final MatchRecordRepo matches;

	private final XpTransactionRepo xp;

	private final GameProperties properties;

	MatchPersistence(MatchRecordRepo matches, XpTransactionRepo xp, GameProperties properties) {
		this.matches = matches;
		this.xp = xp;
		this.properties = properties;
	}

	@Transactional
	public void created(UUID matchId, UUID player1, UUID player2, Instant at) {
		Trace.call(Area.DB, "MatchPersistence.created");
		matches.save(new MatchRecord(matchId, player1, player2, at));
	}

	@Transactional
	public void started(UUID matchId, Instant at) {
		Trace.call(Area.DB, "MatchPersistence.started");
		matches.findById(matchId).ifPresent((m) -> m.start(at));
	}

	@Transactional
	public boolean finished(Result result, Instant at) {
		Trace.call(Area.DB, "MatchPersistence.finished");
		MatchRecord record = matches.findById(result.matchId()).orElse(null);
		if (record == null || record.getStatus() == MatchRecord.Status.FINISHED
				|| record.getStatus() == MatchRecord.Status.CANCELLED) {
			return false;
		}
		record.finish(result, at);
		if (!result.cancelled()) {
			xp.save(new XpTransaction(result.player1().id(), result.matchId(), xpFor(result, result.player1().id()), at));
			xp.save(new XpTransaction(result.player2().id(), result.matchId(), xpFor(result, result.player2().id()), at));
		}
		return true;
	}

	int xpFor(Result result, UUID playerId) {
		if (result.cancelled()) {
			return 0;
		}
		if (result.winnerId() == null) {
			return properties.drawXp();
		}
		return result.winnerId().equals(playerId) ? properties.winXp() : properties.lossXp();
	}

}
