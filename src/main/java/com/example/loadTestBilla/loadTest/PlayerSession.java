package com.example.loadTestBilla.loadTest;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Data
@Builder
@AllArgsConstructor
public class PlayerSession {
    private volatile String token;
    private volatile WebSocket webSocket;

    private final Listener listener;
    private final HttpClient http;
    private final String name;
    private final Metrics metrics;
    private final ScheduledExecutorService scheduler;
    private final String baseUrl;

    public CompletableFuture<Void> start(){
        CompletableFuture<Void> registered = (token!=null)?CompletableFuture.completedFuture(null)
                : Main.register(http,baseUrl, name).thenAccept((reply)->token = reply.get("token").asString());
        registered
                .thenCompose((reply)-> Main.connect(http,baseUrl, listener, token))
                .thenCompose(ws -> {
                    webSocket = ws;
                    return listener.connected.orTimeout(10, TimeUnit.SECONDS);
                })
                .thenCompose(ws -> {
                    return Main.joinQueue(http,baseUrl, listener, token);
                })
                .thenRun(() -> scheduler.schedule(this::checkMatchFound, 30, TimeUnit.SECONDS))
                .exceptionally(error -> {
                    metrics.count("error:session-start");
                    listener.done.completeExceptionally(error);
                    return null;
                });
        listener.done.whenComplete((v, error) -> {
            WebSocket ws = webSocket;
            if(ws != null) ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        });
        return listener.done;
    }
    private void checkMatchFound(){
        if(!listener.matchFound && !listener.done.isDone()){
            metrics.count("timeout:match-found");
            listener.done.complete(null);
        }
    }

}
