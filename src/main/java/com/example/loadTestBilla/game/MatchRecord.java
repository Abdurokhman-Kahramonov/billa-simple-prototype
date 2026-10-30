package com.example.loadTestBilla.game;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;


@Entity
@Table(name = "match_record", indexes = { @Index(name = "ix_match_p1", columnList = "player1_id"),
		@Index(name = "ix_match_p2", columnList = "player2_id") })
@Getter
@NoArgsConstructor
public class MatchRecord {

	public enum Status {
		WAITING_READY, IN_PROGRESS, FINISHED, CANCELLED
	}

	@Id
	private UUID id;

	@Column(name = "player1_id", nullable = false)
	private UUID player1Id;

	@Column(name = "player2_id", nullable = false)
	private UUID player2Id;

	@Enumerated(EnumType.STRING)
	@Column(name = "status", nullable = false, length = 16)
	private Status status;

	@Column(name = "winner_id")
	private UUID winnerId;

	@Column(name = "player1_reps", nullable = false)
	private int player1Reps;

	@Column(name = "player2_reps", nullable = false)
	private int player2Reps;

	@Column(name = "finish_reason", length = 16)
	private String finishReason;

	@Column(name = "created_at", nullable = false)
	private Instant createdAt;

	@Column(name = "started_at")
	private Instant startedAt;

	@Column(name = "finished_at")
	private Instant finishedAt;

	MatchRecord(UUID id, UUID player1Id, UUID player2Id, Instant createdAt) {
		this.id = id;
		this.player1Id = player1Id;
		this.player2Id = player2Id;
		this.createdAt = createdAt;
		this.status = Status.WAITING_READY;
	}

	void start(Instant at) {
		this.status = Status.IN_PROGRESS;
		this.startedAt = at;
	}

	void finish(MatchStore.Result result, Instant at) {
		this.status = result.cancelled() ? Status.CANCELLED : Status.FINISHED;
		this.winnerId = result.winnerId();
		this.player1Reps = result.player1Reps();
		this.player2Reps = result.player2Reps();
		this.finishReason = result.reason();
		this.finishedAt = at;
	}

}
