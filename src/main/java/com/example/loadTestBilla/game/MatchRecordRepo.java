package com.example.loadTestBilla.game;

import java.util.List;
import java.util.UUID;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface MatchRecordRepo extends JpaRepository<MatchRecord, UUID> {

	@Query("""
			select m from MatchRecord m
			where (m.player1Id = :playerId or m.player2Id = :playerId)
			  and m.status = com.example.loadTestBilla.game.MatchRecord.Status.FINISHED
			order by m.finishedAt desc""")
	List<MatchRecord> recentFinished(UUID playerId, Limit limit);

}
