package com.example.loadTestBilla.loadTest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServerScraper {
    private static final List<String> GAUGES = List.of(
            "hikaricp_connections_active", "hikaricp_connections_pending",
            "game_ws_sessions", "process_cpu_usage");
    private static final List<String> COUNTERS = List.of(
            "game_matches_persist_failures_total", "game_ws_send_failures_total",
            "game_matches_opened_total", "jvm_gc_pause_seconds_count", "jvm_gc_pause_seconds_sum"
    );
    private final HttpClient http;
    private final List<String> baseUrls;
    private final Metrics metrics;
    private final Map<String, Double> firstCounters = new ConcurrentHashMap<>();
    private final Map<String, Double> lastCounters = new ConcurrentHashMap<>();

    public ServerScraper(HttpClient http, List<String> baseUrls, Metrics metrics){
        this.http = http;
        this.baseUrls = baseUrls;
        this.metrics = metrics;
    }
    public void scrapeOnce(){
        for(String baseUrl: baseUrls){
            HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl+"/actuator/prometheus"))
                    .timeout(Duration.ofSeconds(2)).GET().build();
            http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> handle(label(baseUrl), response.body()))
                    .exceptionally(error -> {
                        metrics.count("harness:scrape-failed");
                        return null;
                    });
        }
    }
    private void handle(String label, String body){
        Map<String, Double> values = parse(body);
        for(String name:GAUGES){
            Double v = values.get(name);
            if(v!=null) metrics.gauge(label + ":" + name, v);
        }
        for(String name: COUNTERS){
            Double v = values.get(name);
            if(v==null) continue;
            firstCounters.putIfAbsent(label+":" + name, v);
            lastCounters.put((label+":" + name), v);
        }
    }
    static Map<String, Double> parse(String text){
        Map<String, Double> sums = new HashMap<>();
        for (String line : text.split("\n")) {
            if (line.isEmpty() || line.startsWith("#")) continue;
            int space = line.lastIndexOf(' ');
            if (space < 0) continue;
            String key = line.substring(0, space);
            int brace = key.indexOf('{');
            String name = brace < 0 ? key : key.substring(0, brace);
            try {
                sums.merge(name, Double.parseDouble(line.substring(space + 1)), Double::sum);
            } catch (NumberFormatException ignored) { }
        }
        return sums;
    }
    public void resetCounters() {
        firstCounters.clear();
        lastCounters.clear();
    }
    public void printCounterDeltas() {
        System.out.println("Server counters (change since warm-up ended):");
        for (String baseUrl : baseUrls) {
            for (String name : COUNTERS) {
                String key = label(baseUrl) + ":" + name;
                double delta = lastCounters.getOrDefault(key, 0.0) - firstCounters.getOrDefault(key, 0.0);
                System.out.println("  " + key + " = " + delta);
            }
        }
    }
    public double totalDelta(String name){
        double total = 0;
        for(String baseUrl:baseUrls){
            String key = label(baseUrl) + ":" + name;
            total += lastCounters.getOrDefault(key, 0.0) - firstCounters.getOrDefault(key, 0.0);
        }
        return total;
    }
    private static String label(String baseUrl){
        return baseUrl.substring(baseUrl.lastIndexOf(':')+1);
    }

}
