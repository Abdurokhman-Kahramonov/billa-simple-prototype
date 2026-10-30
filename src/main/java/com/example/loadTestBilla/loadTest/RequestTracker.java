package com.example.loadTestBilla.loadTest;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.WebSocket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestTracker {
    private record Pending(String type, long sentAtNanos, String request) {}

    private final String label;
    private final Metrics metrics;
    private final AtomicInteger nextId = new AtomicInteger(1);
    private final Map<Integer, Pending> pending = new ConcurrentHashMap<>();

    public RequestTracker(String label, Metrics metrics) {
        this.label = label;
        this.metrics = metrics;
    }
    public void sendFrame(WebSocket ws, String type, String matchId, long startedAtNano){
        int id = nextId.getAndIncrement();
        String json = "{\"type\":\"" + type + "\",\"matchId\":\"" + matchId + "\",\"id\":" + id + "}";
        pending.put(id, new Pending(type, startedAtNano , json));
        ws.sendText(json, true);
    }
    public void sendFrame(WebSocket ws, String type, String matchId) {
        sendFrame(ws, type, matchId, System.nanoTime());
    }

    public void received(int id, String replyType, String replyBody) {
        Pending p = pending.remove(id);
        if (p == null) {
            metrics.count("harness:unknown-reply-id");
            return;
        }
        long took = System.nanoTime() - p.sentAtNanos();
        metrics.record(p.type() + "_TO_ACK", took);
        metrics.count("frame:" + replyType);
        print(id, p.type(), p.request(), replyType + " after " + ms(took) + " ms, " + replyBody);
    }
    public void failed(int id, String code, String replyBody){
        Pending p = pending.remove(id);
        if(p == null){
            metrics.count("harness:unknown-reply-id");
            return;
        }
        long took = System.nanoTime() - p.sentAtNanos();
        print(id, p.type(), p.request(), "ERROR "+code+" after "+ ms(took)+" ms, "+ replyBody);
    }
    public CompletableFuture<HttpResponse<String>> sendHttp(HttpClient client, HttpRequest request,
                                                            String signal, String requestBody) {
        int id = nextId.getAndIncrement();
        String what = request.method() + " " + request.uri().getPath();
        String sentText = "body: " + (requestBody == null ? "(none)" : requestBody);
        pending.put(id, new Pending(signal, System.nanoTime(), sentText));

        return client.sendAsync(request, BodyHandlers.ofString()).whenComplete((response, error) -> {
            Pending p = pending.remove(id);
            long took = System.nanoTime() - p.sentAtNanos();
            if (error != null) {
                metrics.count("error:http:" + what);
                print(id, what, sentText, "FAILED after " + ms(took) + " ms: " + error.getMessage());
                return;
            }
            metrics.record(signal, took);
            metrics.count("http:" + what + ":" + response.statusCode());
            print(id, what, sentText,
                    response.statusCode() + " after " + ms(took) + " ms, body: " + response.body());
        });
    }

    public int unanswered() {
        return pending.size();
    }

    private void print(int id, String what, String sent, String received) {
        System.out.println(label + " #" + id + " " + what
                + "\n   -> " + sent
                + "\n   <- " + received
                + "\n");
    }

    private static double ms(long nanos) {
        return nanos / 1_000_000.0;
    }
}
