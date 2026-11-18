package com.example.loadTestBilla.loadTest;

import tools.jackson.databind.JsonNode;

import java.net.http.WebSocket;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    private volatile int targetReps;
    private final String baseUrl;
    private volatile String matchId;
    private volatile String opponentId;
    public final CompletableFuture<Void> connected = new CompletableFuture<>();
    private final AtomicInteger acceptedReps = new AtomicInteger();
    private final AtomicBoolean finished = new AtomicBoolean();


    public Listener(String label, Metrics metrics, ScheduledExecutorService scheduler, String baseUrl) {
        this.label = label;
        this.metrics = metrics;
        this.tracker = new RequestTracker(label, metrics);
        this.scheduler = scheduler;
        this.baseUrl = baseUrl;
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
            case "READY_ACK", "PONG" ->
                    tracker.received(frame.path("id").asInt(), type, frame.toString());
            case "REP_REJECTED" -> {
                tracker.received(frame.path("id").asInt(), type, frame.toString());
                onRepRejected(ws, frame);
            }
            case "REP_ACCEPTED" ->{
                tracker.received(frame.path("id").asInt(), type, frame.toString());
                onRepAccepted(frame);
            }
            case "OPPONENT_REP" -> onOpponentRep(frame);
            case "MATCH_FOUND" -> { log(frame); onMatchFound(ws, frame); }
            case "MATCH_STARTED" -> { log(frame); onMatchStarted(ws, frame); }
            case "CONNECTED" -> {log(frame); playerId = frame.path("playerId").asString(); connected.complete(null);}
            case "MATCH_FINISHED" -> {log(frame); onMatchFinished(frame);}
            case "ERROR" -> {log(frame); onMatchError(frame);}
            default -> log(frame);
        }
    }
    private void onRepRejected(WebSocket ws, JsonNode frame){
        if(!"too-fast".equals(frame.path("reason").asString())) return;
        if(finished.get() || acceptedReps.get() >= targetReps) return;
        reps.add(scheduler.schedule(
                () -> tracker.sendFrame(ws, "REP", matchId), 350, TimeUnit.MILLISECONDS));
    }

    private void recordLag(long nanos){
        String kind = Main.matchServers.get(matchId).size() == 1 ? "SAME_SERVER" : "DIFFERENT_SERVER";
        metrics.record("REPLICA_LAG: "+kind, nanos);
    }
    private void onOpponentRep(JsonNode frame){
        if(frame.path("count").asInt() >= targetReps){
            reps.forEach(f->f.cancel(false));
        }
        String key = matchId + ":" + opponentId + ":" + frame.path("count").asInt();
        long now = System.nanoTime();
        Long iSaw = Main.repAcceptedAt.remove(key);
        if(iSaw != null) recordLag(now - iSaw);
        else Main.opponentRepAt.put(key, now);
    }
    private void onRepAccepted(JsonNode frame){
        acceptedReps.incrementAndGet();
        String key = matchId + ":" + playerId + ":" + frame.path("count").asInt();
        long now = System.nanoTime();
        Long opponentSaw = Main.opponentRepAt.remove(key);
        if(opponentSaw != null) recordLag(opponentSaw - now);
        else Main.repAcceptedAt.put(key, now);
    }

    private void onMatchError(JsonNode frame){
        String code = frame.path("code").asString();
        String type = frame.has("id")
                ? tracker.failed(frame.path("id").asInt(), code, frame.toString())
                : null;
        if ("not-in-match".equals(code) && "REP".equals(type)) {
            if (finished.get()) {
                metrics.count("late-rep");
            } else {
                scheduler.schedule(() ->
                                metrics.count(finished.get() ? "late-rep" : "error:not-in-match"),
                        2, TimeUnit.SECONDS);
            }
            return;
        }
        metrics.count("error:" + code);
    }


    private void log(JsonNode frame) {
        System.out.println(label + " <- " + frame + "\n");
    }
    private void onMatchFinished(JsonNode frame){
        if(!finished.compareAndSet(false, true)){
            metrics.count("error:match-finished-twice");
            return;
        }
        if(frame.path("yourReps").asInt() != acceptedReps.get()){
            metrics.count("error:match-finished-mismatch");
        }
        reps.forEach(f -> f.cancel(false));
        boolean iWon = "TARGET_REACHED".equals(frame.path("reason").asString())
                && playerId != null && playerId.equals(frame.path("winnerId").asString());
        if(iWon){
            metrics.record("LAST_REP_TO_FINISHED", System.nanoTime() - lastRepIntended);
        }
        done.complete(null);

    }
    private void onMatchFound(WebSocket ws, JsonNode frame) {
        matchId = frame.path("matchId").asString();
        opponentId = frame.path("opponent").path("id").asString();
        Main.matchServers.computeIfAbsent(matchId, k -> ConcurrentHashMap.newKeySet()).add(baseUrl);


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
        targetReps = frame.path("target").asInt();
        int target = targetReps;
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
