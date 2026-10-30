package com.example.loadTestBilla.player;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "player")
@Getter
@NoArgsConstructor
public class Player {

	@Id
	@GeneratedValue(strategy = GenerationType.UUID)
	private UUID id;

	@Column(name = "name", nullable = false, length = 32)
	private String name;


	@Column(name = "token", nullable = false, unique = true, length = 64)
	private String token;

	@Column(name = "created_at", nullable = false)
	private Instant createdAt;

	public Player(String name, String token, Instant createdAt) {
		this.name = name;
		this.token = token;
		this.createdAt = createdAt;
	}

}
