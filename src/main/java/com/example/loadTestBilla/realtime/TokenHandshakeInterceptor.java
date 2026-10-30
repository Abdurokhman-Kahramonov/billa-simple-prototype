package com.example.loadTestBilla.realtime;

import java.util.Map;
import java.util.Optional;

import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;
import com.example.loadTestBilla.player.Player;
import com.example.loadTestBilla.player.PlayerRepo;

import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

@Component
class TokenHandshakeInterceptor implements HandshakeInterceptor {

	static final String PLAYER_ID = "playerId";

	static final String PLAYER_NAME = "playerName";

	private final PlayerRepo players;

	TokenHandshakeInterceptor(PlayerRepo players) {
		this.players = players;
	}

	@Override
	public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
			Map<String, Object> attributes) {
		Trace.call(Area.SOCKET, "TokenHandshakeInterceptor.beforeHandshake");
		String token = UriComponentsBuilder.fromUri(request.getURI()).build().getQueryParams().getFirst("token");
		Optional<Player> player = (token == null) ? Optional.empty() : players.findByToken(token);
		if (player.isEmpty()) {
			response.setStatusCode(HttpStatus.UNAUTHORIZED);
			return false;
		}
		attributes.put(PLAYER_ID, player.get().getId());
		attributes.put(PLAYER_NAME, player.get().getName());
		return true;
	}

	@Override
	public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler handler,
			Exception exception) {

	}

}
