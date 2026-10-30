package com.example.loadTestBilla.loadTest;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class Main {
    public static final String BASE_URL = "http://localhost:8081";
    public static final String WS_BASE_URL = "ws://localhost:8081";
    public static final ObjectMapper JSON = new ObjectMapper();
    public static final Metrics metrics = new Metrics();
    private static final RequestTracker httpTracker = new RequestTracker("HTTP", metrics);

    static int concurrency = Integer.getInteger("load.concurrency", 4);
    static int warmupSeconds = Integer.getInteger("load.warmup", 10);
    static int steadySeconds = Integer.getInteger("load.steady", 30);
    static long seed = Long.getLong("load.seed", 42L);


    public static void main(String[] args) throws InterruptedException {
        double rate = concurrency / 9.0;
        Random random = new Random(seed);
        List<Long> arrivals = new ArrayList<>();
        double at = 0;
        while(at<warmupSeconds+steadySeconds){
            at += -Math.log(1 - random.nextDouble()) / rate;
            arrivals.add((long)(at*1_000_000_000L));
        }
        System.out.println("seed=" + seed + " arrivals=" + arrivals.size());

        ExecutorService executor = Executors.newFixedThreadPool(8);
        HttpClient http = HttpClient.newBuilder().executor(executor)
                .connectTimeout(Duration.ofSeconds(10)).build();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

        Listener l1 = new Listener("Player1", metrics, scheduler);
        Listener l2 = new Listener("Player2", metrics, scheduler);

        PlayerSession s1 = PlayerSession.builder()
                .http(http)
                .scheduler(scheduler)
                .metrics(metrics)
                .name("Player-1")
                .listener(l1)
                .build();
        PlayerSession s2 = PlayerSession.builder()
                .http(http)
                .scheduler(scheduler)
                .metrics(metrics)
                .name("Player-2")
                .listener(l2)
                .build();

        CompletableFuture.allOf(s1.start(), s2.start()).orTimeout(60, TimeUnit.SECONDS).join();

        metrics.printMetrics();
        System.out.println("Unanswered requests: Player1=" + l1.unanswered() + ", Player2=" + l2.unanswered()
                + ", HTTP=" + httpTracker.unanswered());
        executor.shutdown();
        scheduler.shutdown();
        System.exit(0);
    }

    static CompletableFuture<JsonNode> register(HttpClient http, String name) {
        String body = "{\"name\":\"" + name + "\"}";
        HttpRequest request = HttpRequest.newBuilder(URI.create(BASE_URL + "/api/players"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return send(http, request, 201, "REGISTER", body);
    }

    static CompletableFuture<JsonNode> joinQueue(HttpClient http, Listener listener, String token) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(BASE_URL + "/api/queue"))
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return send(http, request, 202, "QUEUE_ACCEPTED", null).thenApply(reply -> {
            listener.startQueueTimer();
            return reply;
        });
    }

    static CompletableFuture<WebSocket> connect(HttpClient http, Listener listener, String token) {
        long start = System.nanoTime();
        return http.newWebSocketBuilder()
                .buildAsync(URI.create(WS_BASE_URL + "/ws?token=" + token), listener)
                .whenComplete((ws, error) -> metrics.record("CONNECT", System.nanoTime() - start));
    }

    private static CompletableFuture<JsonNode> send(HttpClient http, HttpRequest request,
                                                    int expectedStatus, String signal, String requestBody) {
        return httpTracker.sendHttp(http, request, signal, requestBody).thenApply(response -> {
            if (response.statusCode() != expectedStatus) {
                throw new IllegalStateException(signal + " failed: " + response.statusCode() + " " + response.body());
            }
            return JSON.readTree(response.body());
        });
    }
}
