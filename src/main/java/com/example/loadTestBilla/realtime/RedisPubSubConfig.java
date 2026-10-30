package com.example.loadTestBilla.realtime;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;


@Configuration
class RedisPubSubConfig {

	@Bean
	RedisMessageListenerContainer redisListener(RedisConnectionFactory connections, SessionRegistry sessions) {
		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connections);
		container.setTaskExecutor(new TaskExecutorAdapter(Executors.newSingleThreadExecutor((r) -> {
			Thread t = new Thread(r, "redis-pubsub-delivery");
			t.setDaemon(true);
			return t;
		})));
		container.addMessageListener(
				(message, pattern) -> sessions.onRemoteMessage(new String(message.getBody(), StandardCharsets.UTF_8)),
				new ChannelTopic(SessionRegistry.channel(sessions.instanceId())));
		return container;
	}

}
