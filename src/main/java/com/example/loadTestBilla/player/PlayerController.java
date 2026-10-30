package com.example.loadTestBilla.player;

import java.util.UUID;

import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/players")
public class PlayerController {

	private final PlayerService players;

	private final XpTransactionRepo xp;

	public PlayerController(PlayerService players, XpTransactionRepo xp) {
		this.players = players;
		this.xp = xp;
	}

	public record RegisterRequest(String name) {
	}

	public record RegisterResponse(UUID id, String name, String token) {
	}

	public record ProfileResponse(UUID id, String name, long xp) {
	}


	@PostMapping
	@ResponseStatus(HttpStatus.CREATED)
	public RegisterResponse register(@RequestBody RegisterRequest request) {
		Trace.call(Area.HTTP, "PlayerController.register");
		Player player = players.register(request.name());
		return new RegisterResponse(player.getId(), player.getName(), player.getToken());
	}

	@GetMapping("/me")
	public ProfileResponse me(@RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String auth) {
		Trace.call(Area.HTTP, "PlayerController.me");
		Player player = players.requireByAuthorization(auth);
		return new ProfileResponse(player.getId(), player.getName(), xp.totalXp(player.getId()));
	}

}
