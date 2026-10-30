package com.example.loadTestBilla.player;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.HexFormat;

import com.example.loadTestBilla.common.ApiException;
import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;
import com.example.loadTestBilla.game.GameProperties;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PlayerService {

	private static final SecureRandom RANDOM = new SecureRandom();

	private final PlayerRepo players;

	private final GameProperties properties;

	public PlayerService(PlayerRepo players, GameProperties properties) {
		this.players = players;
		this.properties = properties;
	}

	@Transactional
	public Player register(String name) {
		Trace.call(Area.DB, "PlayerService.register");
		String trimmed = (name == null) ? "" : name.trim();
		if (trimmed.isEmpty() || trimmed.length() > 32) {
			throw new ApiException(HttpStatus.BAD_REQUEST, "invalid-name");
		}
		if (properties.registrationDbDelayMs() > 0) {
			players.sleep(properties.registrationDbDelayMs() / 1000.0);
		}
		byte[] bytes = new byte[24];
		RANDOM.nextBytes(bytes);
		return players.save(new Player(trimmed, HexFormat.of().formatHex(bytes), Instant.now()));
	}


	@Transactional(readOnly = true)
	public Player requireByAuthorization(String authorization) {
		Trace.call(Area.DB, "PlayerService.requireByAuthorization");
		if (authorization == null || !authorization.startsWith("Bearer ")) {
			throw new ApiException(HttpStatus.UNAUTHORIZED, "unauthorized");
		}
		return players.findByToken(authorization.substring("Bearer ".length()).trim())
			.orElseThrow(() -> new ApiException(HttpStatus.UNAUTHORIZED, "unauthorized"));
	}

}
