package com.example.loadTestBilla.game;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;
import com.example.loadTestBilla.player.Player;
import com.example.loadTestBilla.player.PlayerService;

import org.springframework.data.domain.Limit;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GameController {

	private final PlayerService players;

	private final MatchmakingService matchmaking;

	private final MatchRecordRepo matches;

	public GameController(PlayerService players, MatchmakingService matchmaking, MatchRecordRepo matches) {
		this.players = players;
		this.matchmaking = matchmaking;
		this.matches = matches;
	}

	@PostMapping("/api/queue")
	public ResponseEntity<Map<String, Object>> join(
			@RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String auth) {
		Trace.call(Area.HTTP, "GameController.join");
		Player player = players.requireByAuthorization(auth);
		matchmaking.enqueue(player.getId(), player.getName());
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("queued", true));
	}

	@DeleteMapping("/api/queue")
	public ResponseEntity<Void> leave(@RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String auth) {
		Trace.call(Area.HTTP, "GameController.leave");
		matchmaking.leave(players.requireByAuthorization(auth).getId());
		return ResponseEntity.noContent().build();
	}

	@GetMapping("/api/queue")
	public Map<String, Object> status(@RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String auth) {
		Trace.call(Area.HTTP, "GameController.status");
		return matchmaking.status(players.requireByAuthorization(auth).getId());
	}

	public record MatchSummary(UUID matchId, UUID opponentId, boolean won, boolean draw, int yourReps,
			int opponentReps, String reason, Instant finishedAt) {
	}

	@GetMapping("/api/players/me/matches")
	public List<MatchSummary> history(@RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String auth,
			@RequestParam(defaultValue = "10") int limit) {
		Trace.call(Area.HTTP, "GameController.history");
		UUID me = players.requireByAuthorization(auth).getId();
		return matches.recentFinished(me, Limit.of(Math.clamp(limit, 1, 50))).stream().map((m) -> {
			boolean first = m.getPlayer1Id().equals(me);
			return new MatchSummary(m.getId(), first ? m.getPlayer2Id() : m.getPlayer1Id(), me.equals(m.getWinnerId()),
					m.getWinnerId() == null, first ? m.getPlayer1Reps() : m.getPlayer2Reps(),
					first ? m.getPlayer2Reps() : m.getPlayer1Reps(), m.getFinishReason(), m.getFinishedAt());
		}).toList();
	}

}
