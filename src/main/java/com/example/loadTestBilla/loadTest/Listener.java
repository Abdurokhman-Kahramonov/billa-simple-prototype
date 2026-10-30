package com.example.loadTestBilla.loadTest;

import tools.jackson.databind.JsonNode;

import java.net.http.WebSocket;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Listener implements WebSocket.Listener {
    private final String label;
    private final Metrics metrics;
    private final RequestTracker tracker;
    private final StringBuilder buffer = new StringBuilder();
    private final Random random = new Random();
    private final AtomicLong queueStarted = new AtomicLong(0);
    private final AtomicLong readySent = new AtomicLong(0);
    private final ScheduledExecutorService scheduler;
    private volatile String playerId;
    public final CompletableFuture<Void> done = new CompletableFuture<>();
    public volatile boolean matchFound;
    private volatile long lastRepIntended;
    private final List<ScheduledFuture<?>> reps = new CopyOnWriteArrayList<>();

    public Listener(String label, Metrics metrics, ScheduledExecutorService scheduler) {
        this.label = label;
        this.metrics = metrics;
        this.tracker = new RequestTracker(label, metrics);
        this.scheduler = scheduler;
    }

    public void startQueueTimer() {
        queueStarted.set(System.nanoTime());
    }

    public int unanswered() {
        return tracker.unanswered();
    }

    @Override
    public void onOpen(WebSocket ws) {
        ws.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
        buffer.append(data);
        if (last) {
            String message = buffer.toString();
            buffer.setLength(0);
            handle(ws, Main.JSON.readTree(message));
        }
        ws.request(1);
        return null;
    }

    private void handle(WebSocket ws, JsonNode frame) {
        String type = frame.path("type").asString();
        switch (type) {
            case "READY_ACK", "REP_ACCEPTED", "REP_REJECTED", "PONG" ->
                    tracker.received(frame.path("id").asInt(), type, frame.toString());
            case "MATCH_FOUND" -> { log(frame); onMatchFound(ws, frame); }
            case "MATCH_STARTED" -> { log(frame); onMatchStarted(ws, frame); }
            case "CONNECTED" -> {log(frame); playerId = frame.path("playerId").asString();}
            case "MATCH_FINISHED" -> {log(frame); onMatchFinished(frame);}
            case "ERROR" -> {log(frame); onMatchError(frame);}
            default -> log(frame);
        }
    }
    private void onMatchError(JsonNode frame){
        String code = frame.path("code").asString();
        metrics.count("error:"+code);
        if(frame.has("id")){
            tracker.failed(frame.path("id").asInt(), code, frame.toString());
        }
    }

    private void log(JsonNode frame) {
        System.out.println(label + " <- " + frame + "\n");
    }
    private void onMatchFinished(JsonNode frame){
        reps.forEach(f -> f.cancel(false));
        boolean iWon = "TARGET_REACHED".equals(frame.path("reason").asString())
                && playerId != null && playerId.equals(frame.path("winnerId").asString());
        if(iWon){
            metrics.record("LAST_REP_TO_FINISHED", System.nanoTime() - lastRepIntended);
        }
        done.complete(null);

    }
    private void onMatchFound(WebSocket ws, JsonNode frame) {
        long started = queueStarted.get();
        matchFound = true;
        if (started != 0) {
            metrics.record("QUEUE_TO_MATCH_FOUND", System.nanoTime() - started);
        }
        readySent.set(System.nanoTime());
        send(ws, "READY", frame.path("matchId").asString());
    }

    private void onMatchStarted(WebSocket ws, JsonNode frame) {
        metrics.record("READY_TO_STARTED", System.nanoTime() - readySent.get());
        int target = frame.path("target").asInt();
        String matchId = frame.path("matchId").asString();
        long start = System.nanoTime();
        long offset = 0;
        for(int k = 1; k <= target; k++){
            offset += (400 + random.nextInt(201)) * 1_000_000L;
            long intended = start + offset;
            if(k == target) lastRepIntended = intended;

            reps.add(scheduler.schedule(() -> {
              metrics.record("GENERATOR_LAG", System.nanoTime() - intended);
              tracker.sendFrame(ws, "REP", matchId, intended);
            }, intended - System.nanoTime(), TimeUnit.NANOSECONDS));

        }
    }

    private void send(WebSocket ws, String type, String matchId) {
        tracker.sendFrame(ws, type, matchId);
    }

    @Override
    public void onError(WebSocket ws, Throwable error) {
        metrics.count("error:websocket");
        done.completeExceptionally(error);
        System.err.println(label + " socket error: " + error.getMessage());
    }
}
