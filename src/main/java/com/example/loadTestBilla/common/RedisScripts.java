package com.example.loadTestBilla.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

@Component
public class RedisScripts {

	private final StringRedisTemplate redis;

	private final MeterRegistry meters;

	private final Map<String, RedisScript<List>> scripts = new ConcurrentHashMap<>();

	public RedisScripts(StringRedisTemplate redis, MeterRegistry meters) {
		this.redis = redis;
		this.meters = meters;
	}

	@SuppressWarnings("unchecked")
	public List<String> run(String name, String lua, List<String> keys, Object... args) {
		RedisScript<List> script = scripts.computeIfAbsent(name, (n) -> new DefaultRedisScript<>(lua, List.class));
		String[] stringArgs = new String[args.length];
		for (int i = 0; i < args.length; i++) {
			stringArgs[i] = String.valueOf(args[i]);
		}
		Timer.Sample sample = Timer.start(meters);
		try {
			List<String> result = redis.execute(script, keys, (Object[]) stringArgs);
			return (result != null) ? result : List.of();
		}
		finally {
			sample.stop(Timer.builder("game.redis.script").tag("script", name).register(meters));
		}
	}

	public StringRedisTemplate template() {
		return redis;
	}

}
