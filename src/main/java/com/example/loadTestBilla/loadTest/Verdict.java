package com.example.loadTestBilla.loadTest;

import java.util.ArrayList;
import java.util.List;

public class Verdict {
    static final double REP_TO_ACK_P95_MS = 100;
    static final double REP_TO_ACK_P99_MS = 200;
    static final double DB_PENDING_MAX = 5;
    static final double GENERATOR_LAG_P99 = 100;
    static final double GENERATOR_CPU_95 = 85;
    static final double MIN_CONCURRENCY_RATIO = 0.85;

    public static String evaluate(Metrics m, ServerScraper s, int targetConcurrency){
        List<String> notGenerated = new ArrayList<>();
        List<String> failures = new ArrayList<>();

        double meanActive = m.gaugeMean("Active Sessions");
        if(!(meanActive >= targetConcurrency*MIN_CONCURRENCY_RATIO))
            notGenerated.add("mean concurrency " + meanActive);
        if(m.gaugePercentile("GENERATOR_CPU_PERCENT", 95) > GENERATOR_CPU_95)
            notGenerated.add("generator cpu p95 >= "+ GENERATOR_CPU_95);
        if(m.percentileMs("GENERATOR_LAG", 99) > GENERATOR_LAG_P99)
            notGenerated.add("generator lag p99 >= "+ GENERATOR_LAG_P99);
        if(m.countOf("error:pool-empty")>0)
            notGenerated.add("account pool ran empty ("+m.countOf("error:pool-empty") + " times)");
        // 2. Did the server miss a limit?
        if (m.percentileMs("REP_TO_ACK", 95) > REP_TO_ACK_P95_MS)
            failures.add("REP_TO_ACK p95 > " + REP_TO_ACK_P95_MS + " ms");
        if (m.percentileMs("REP_TO_ACK", 99) > REP_TO_ACK_P99_MS)
            failures.add("REP_TO_ACK p99 > " + REP_TO_ACK_P99_MS + " ms");
        for (String name : m.gaugeNamesEndingWith(":hikaricp_connections_pending")) {
            if (m.gaugePercentile(name, 100) >= DB_PENDING_MAX)
                failures.add(name + " max >= " + DB_PENDING_MAX);
        }

        m.countsStartingWith("timeout:").forEach((k, v) -> failures.add(k + " = " + v));
        m.countsStartingWith("error:").forEach((k, v) -> {
            if (!k.equals("error:pool-empty")) failures.add(k + " = " + v);
        });
        m.countsStartingWith("http:").forEach((k, v) -> {
            if (k.matches(".*:5\\d\\d")) failures.add(k + " = " + v);
        });
        if (s.totalDelta("game_matches_persist_failures_total") > 0)
            failures.add("matches failed to persist");
        if (s.totalDelta("game_ws_send_failures_total") > 0)
            failures.add("websocket sends failed");

        if (m.countOf("frame:REP_ACCEPTED") == 0)
            failures.add("no REP was accepted: nothing was measured");

        notGenerated.forEach(r->
                System.out.println("NOT GENERATED because: " + r));
        failures.forEach(r->
                System.out.println("FAILURE because: " + r));
        if(!notGenerated.isEmpty()) return "NOT GENERATED";
        return failures.isEmpty()?"PASS":"FAILURE";
    }
}
