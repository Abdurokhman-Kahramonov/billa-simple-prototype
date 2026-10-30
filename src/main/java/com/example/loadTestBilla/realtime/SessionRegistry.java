package com.example.loadTestBilla.realtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.example.loadTestBilla.common.RedisScripts;
import com.example.loadTestBilla.common.Trace;
import com.example.loadTestBilla.common.Trace.Area;
import com.example.loadTestBilla.game.GameProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;


@Component
public class SessionRegistry {

	private static final Logger log = LoggerFactory.getLogger(SessionRegistry.class);

	private static final int SEND_TIME_LIMIT_MS = 5_000;

	private static final int BUFFER_LIMIT_BYTES = 256 * 1024;


	private static final String CLAIM = """
			local old = redis.call('GET', KEYS[1])
			redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
			return {old or ''}
			""";


	private static final String RELEASE = """
			if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('DEL', KEYS[1]) return {'1'} end
			return {}
			""";

	private static final String HEARTBEAT = """
			for _, k in ipairs(KEYS) do
			  local owner = redis.call('GET', k)
			  if owner == ARGV[1] then redis.call('PEXPIRE', k, ARGV[2])
			  elseif not owner then redis.call('SET', k, ARGV[1], 'PX', ARGV[2]) end
			end
			return {}
			""";

	private final Map<UUID, WebSocketSession> local = new ConcurrentHashMap<>();

	private final RedisScripts redis;

	private final JsonMapper json;

	private final String me;

	private final long ttlMs;

	private final Counter sendFailures;

	private final Counter routedRemote;

	private final Counter routeDropped;

	public SessionRegistry(RedisScripts redis, JsonMapper json, GameProperties properties, MeterRegistry meters) {
		this.redis = redis;
		this.json = json;
		this.me = properties.instanceId();
		this.ttlMs = properties.sessionTtl().toMillis();
		this.sendFailures = meters.counter("game.ws.send.failures");
		this.routedRemote = meters.counter("game.ws.routed", "to", "remote");
		this.routeDropped = meters.counter("game.ws.routed", "to", "nobody");
		Gauge.builder("game.ws.sessions", this, (r) -> r.local.size()).register(meters);
		log.info("This instance is '{}'", me);
	}

	public static String channel(String instanceId) {
		return "instance:" + instanceId;
	}

	static String sessionKey(UUID playerId) {
		return "session:" + playerId;
	}

	public String instanceId() {
		return me;
	}

	WebSocketSession register(UUID playerId, WebSocketSession raw) {
		Trace.call(Area.SOCKET, "SessionRegistry.register");
		WebSocketSession session = new ConcurrentWebSocketSessionDecorator(raw, SEND_TIME_LIMIT_MS, BUFFER_LIMIT_BYTES);
		close(local.put(playerId, session));
		String previousOwner = redis.run("session-claim", CLAIM, List.of(sessionKey(playerId)), me, ttlMs).get(0);
		if (!previousOwner.isEmpty() && !previousOwner.equals(me)) {
			redis.template().convertAndSend(channel(previousOwner), "KICK\n" + playerId);
		}
		return session;
	}

	boolean unregister(UUID playerId, WebSocketSession session) {
		Trace.call(Area.SOCKET, "SessionRegistry.unregister");
		if (!local.remove(playerId, session)) {
			return false;
		}
		try {
			return !redis.run("session-release", RELEASE, List.of(sessionKey(playerId)), me).isEmpty();
		}
		catch (RuntimeException ex) {
			log.warn("Could not release the session of {}", playerId, ex);
			return true;
		}
	}

	public boolean isConnected(UUID playerId) {
		Trace.call(Area.SOCKET, "SessionRegistry.isConnected");
		WebSocketSession session = local.get(playerId);
		if (session != null && session.isOpen()) {
			return true;
		}
		return Boolean.TRUE.equals(redis.template().hasKey(sessionKey(playerId)));
	}

	public void send(UUID playerId, Map<String, Object> frame) {
		Trace.call(Area.SOCKET, "SessionRegistry.send", frame.get("type"));
		String text = json.writeValueAsString(frame);
		if (deliverLocally(playerId, text)) {
			return;
		}
		try {
			String owner = redis.template().opsForValue().get(sessionKey(playerId));
			if (owner == null || owner.equals(me)) {
				routeDropped.increment();
				return;
			}
			redis.template().convertAndSend(channel(owner), "FRAME\n" + playerId + "\n" + text);
			routedRemote.increment();
		}
		catch (RuntimeException ex) {
			sendFailures.increment();
			log.debug("Routing to {} failed", playerId, ex);
		}
	}

	void onRemoteMessage(String body) {
		Trace.call(Area.SOCKET, "SessionRegistry.onRemoteMessage");
		String[] parts = body.split("\n", 3);
		UUID playerId = UUID.fromString(parts[1]);
		if ("FRAME".equals(parts[0])) {
			if (!deliverLocally(playerId, parts[2])) {
				routeDropped.increment();
			}
		}
		else if ("KICK".equals(parts[0])) {
			close(local.get(playerId));
		}
	}

	@Scheduled(fixedDelayString = "${game.session-heartbeat}")
	void heartbeat() {
		if (!local.isEmpty()) {
			Trace.call(Area.SOCKET, "SessionRegistry.heartbeat", local.size() + " sockets");
		}
		List<String> keys = new ArrayList<>();
		for (UUID playerId : local.keySet()) {
			keys.add(sessionKey(playerId));
			if (keys.size() == 200) {
				refresh(keys);
				keys = new ArrayList<>();
			}
		}
		if (!keys.isEmpty()) {
			refresh(keys);
		}
	}

	private void refresh(List<String> keys) {
		try {
			redis.run("session-heartbeat", HEARTBEAT, keys, me, ttlMs);
		}
		catch (RuntimeException ex) {
			log.warn("Session heartbeat failed for {} sockets", keys.size(), ex);
		}
	}

	private boolean deliverLocally(UUID playerId, String text) {
		WebSocketSession session = local.get(playerId);
		if (session == null || !session.isOpen()) {
			return false;
		}
		try {
			session.sendMessage(new TextMessage(text));
		}
		catch (IOException | RuntimeException ex) {
			sendFailures.increment();
			log.debug("Send to {} failed", playerId, ex);
		}
		return true;
	}

	private static void close(WebSocketSession session) {
		if (session == null) {
			return;
		}
		try {
			session.close(new CloseStatus(4000, "replaced"));
		}
		catch (IOException ex) {
			log.debug("Closing a replaced socket failed", ex);
		}
	}

}
