package com.example.loadTestBilla.realtime;

import java.util.Set;
import java.util.UUID;

import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;
import com.example.loadTestBilla.game.MatchService;
import com.example.loadTestBilla.game.MatchmakingService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;


@Component
class GameSocketHandler extends TextWebSocketHandler {

	private static final Logger log = LoggerFactory.getLogger(GameSocketHandler.class);

	private static final Set<String> KNOWN = Set.of("PING", "READY", "REP", "LEAVE");

	private final SessionRegistry sessions;

	private final MatchService matches;

	private final MatchmakingService matchmaking;

	private final JsonMapper json;

	private final MeterRegistry meters;

	GameSocketHandler(SessionRegistry sessions, MatchService matches, MatchmakingService matchmaking, JsonMapper json,
			MeterRegistry meters) {
		this.sessions = sessions;
		this.matches = matches;
		this.matchmaking = matchmaking;
		this.json = json;
		this.meters = meters;
	}

	@Override
	public void afterConnectionEstablished(WebSocketSession raw) {
		Trace.call(Area.SOCKET, "GameSocketHandler.afterConnectionEstablished");
		UUID playerId = playerId(raw);
		WebSocketSession session = sessions.register(playerId, raw);
		raw.getAttributes().put("decorated", session);
		sessions.send(playerId, Frames.of("CONNECTED", "playerId", playerId));
	}

	@Override
	protected void handleTextMessage(WebSocketSession raw, TextMessage message) {
		UUID playerId = playerId(raw);
		long start = System.nanoTime();
		String type = "unknown";
		try {
			JsonNode frame = json.readTree(message.getPayload());
			type = frame.path("type").asString("");
			Trace.call(Area.SOCKET, "GameSocketHandler.handleTextMessage", type);
			Object id = frame.has("id") ? frame.get("id") : null;
			switch (type) {
				case "PING" -> sessions.send(playerId, Frames.of("PONG", "id", id));
				case "READY" -> withMatchId(playerId, frame, id, (matchId) -> matches.ready(playerId, matchId, id));
				case "REP" -> withMatchId(playerId, frame, id, (matchId) -> matches.rep(playerId, matchId, id));
				case "LEAVE" -> {
					matchmaking.leave(playerId);
					matches.leave(playerId);
				}
				default -> sessions.send(playerId, Frames.of("ERROR", "id", id, "code", "unknown-type"));
			}
		}
		catch (JacksonException ex) {
			sessions.send(playerId, Frames.of("ERROR", "code", "bad-json"));
		}
		catch (RuntimeException ex) {

			log.warn("Frame {} from {} failed", type, playerId, ex);
			sessions.send(playerId, Frames.of("ERROR", "code", "server-error"));
		}
		finally {
			Timer.builder("game.ws.dispatch").tag("type", KNOWN.contains(type) ? type : "unknown").register(meters)
				.record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public void afterConnectionClosed(WebSocketSession raw, CloseStatus status) {
		Trace.call(Area.SOCKET, "GameSocketHandler.afterConnectionClosed", status.getCode());
		UUID playerId = playerId(raw);
		Object decorated = raw.getAttributes().get("decorated");
		// If this socket was already replaced by a newer one, the player has not left.
		if (decorated instanceof WebSocketSession session && sessions.unregister(playerId, session)) {
			matchmaking.leave(playerId);
			matches.leave(playerId);
		}
	}

	private void withMatchId(UUID playerId, JsonNode frame, Object id, java.util.function.Consumer<UUID> action) {
		try {
			action.accept(UUID.fromString(frame.path("matchId").asString("")));
		}
		catch (IllegalArgumentException ex) {
			sessions.send(playerId, Frames.of("ERROR", "id", id, "code", "bad-match-id"));
		}
	}

	private static UUID playerId(WebSocketSession session) {
		return (UUID) session.getAttributes().get(TokenHandshakeInterceptor.PLAYER_ID);
	}

}
