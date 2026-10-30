package com.example.loadTestBilla.player;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.NoArgsConstructor;


@Entity
@Table(name = "xp_transaction", indexes = @Index(name = "ix_xp_player", columnList = "player_id"),
		uniqueConstraints = @UniqueConstraint(name = "ux_xp_player_match", columnNames = { "player_id", "match_id" }))
@Getter
@NoArgsConstructor
public class XpTransaction {

	@Id
	@GeneratedValue(strategy = GenerationType.UUID)
	private UUID id;

	@Column(name = "player_id", nullable = false)
	private UUID playerId;

	@Column(name = "match_id", nullable = false)
	private UUID matchId;

	@Column(name = "amount", nullable = false)
	private int amount;

	@Column(name = "created_at", nullable = false)
	private Instant createdAt;

	public XpTransaction(UUID playerId, UUID matchId, int amount, Instant createdAt) {
		this.playerId = playerId;
		this.matchId = matchId;
		this.amount = amount;
		this.createdAt = createdAt;
	}

}
