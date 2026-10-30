package com.example.loadTestBilla.game;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties("game")
public record GameProperties(String instanceId, Duration sessionTtl, Duration sessionHeartbeat, int targetReps,
		Duration minRepInterval, Duration countdown, Duration readyTimeout, Duration maxMatchDuration,
		Duration matchmakingInterval, int winXp, int lossXp, int drawXp, long registrationDbDelayMs) {
}
