package com.example.loadTestBilla.player;

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface PlayerRepo extends JpaRepository<Player, UUID> {

	Optional<Player> findByToken(String token);

	@Query(value = "select pg_sleep(:seconds)", nativeQuery = true)
	void sleep(double seconds);

}
