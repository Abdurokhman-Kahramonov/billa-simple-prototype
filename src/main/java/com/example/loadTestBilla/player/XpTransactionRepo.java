package com.example.loadTestBilla.player;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface XpTransactionRepo extends JpaRepository<XpTransaction, UUID> {

	@Query("select coalesce(sum(x.amount), 0) from XpTransaction x where x.playerId = :playerId")
	long totalXp(UUID playerId);

}
