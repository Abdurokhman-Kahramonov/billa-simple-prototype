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
import java.util.concurrent.atomic.AtomicInteger;

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
        System.out.printf("Settings: concurrency=%d, warmupsec=%d, steadySec=%d, seed=%d\n",concurrency, warmupSeconds, steadySeconds, seed);

        double rate = concurrency / 9.0;
        Random random = new Random(seed);
        List<Long> arrivals = new ArrayList<>();
        double at = 0;
        while(at<warmupSeconds+steadySeconds){
            at += -Math.log(1 - random.nextDouble()) / rate;
            if (at >= warmupSeconds + steadySeconds) break;
            arrivals.add((long)(at*1_000_000_000L));
        }
        System.out.println("seed=" + seed + " arrivals=" + arrivals.size());

        ExecutorService executor = Executors.newFixedThreadPool(8);
        HttpClient http = HttpClient.newBuilder().executor(executor)
                .connectTimeout(Duration.ofSeconds(10)).build();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(6);

        long start = System.nanoTime();
        List<CompletableFuture<Void>> sessions = new CopyOnWriteArrayList<>();
        AtomicInteger pairCounter = new AtomicInteger();
        for(long offset : arrivals){
            long intended = start + offset;
            scheduler.schedule(() -> {
                metrics.record("GENERATOR_LAG", System.nanoTime() - intended);
                int n = pairCounter.incrementAndGet();
                for(String suffix : List.of("a", "b")){
                    String name = "P-"+n+"-"+suffix;
                    PlayerSession session = PlayerSession.builder()
                            .http(http)
                            .scheduler(scheduler)
                            .metrics(metrics)
                            .name(name)
                            .listener(new Listener(name, metrics, scheduler))
                            .build();
                    sessions.add(session.start());
                }
            }, intended - System.nanoTime(), TimeUnit.NANOSECONDS);
        }
        long endNanos = start + (warmupSeconds+steadySeconds)*1_000_000_000L;
        long sleepMs = (endNanos - System.nanoTime()) / 1_000_000;
        if(sleepMs>0) Thread.sleep(sleepMs);
        try{
            CompletableFuture.allOf(sessions.toArray(new CompletableFuture[0]))
                    .orTimeout(60, TimeUnit.SECONDS)
                    .join();
        } catch (Exception e){
            System.out.println("Some sessions failed or timed out; "+e);
        }
        metrics.printMetrics();
        System.out.println("Sessions started: " + sessions.size()
                + ", HTTP unanswered=" + httpTracker.unanswered());
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
