package com.example.loadTestBilla.loadTest;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

@Data
@NoArgsConstructor
public class Metrics {
    private final Map<String, List<Long>> samples = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();

    public void record(String signalName, long value){
        samples.computeIfAbsent(signalName, k -> Collections.synchronizedList(new ArrayList<>())).add(value);
    }

    public void count(String key){
        counts.computeIfAbsent(key, k-> new LongAdder()).increment();
    }
    public void printMetrics(){
        System.out.println("Metrics:");
        for(String name : samples.keySet()){
            List<Long> sorted = new ArrayList<>(samples.get(name));
            Collections.sort(sorted);
            System.out.println(name + " (" + sorted.size() + " samples)"
                    + "  p50=" + ms(percentile(sorted, 50))
                    + "  p95=" + ms(percentile(sorted, 95))
                    + "  p99=" + ms(percentile(sorted, 99))
                    + "  max=" + ms(sorted.get(sorted.size() - 1)) + " ms");
        }
        for(String key: counts.keySet()){
            System.out.println(key + ": " + counts.get(key).sum());
        }

    }
    private static long percentile(List<Long> sorted, double p){
        int position = (int) Math.ceil(p / 100.0 * sorted.size());
        return sorted.get(Math.max(1, position) - 1);
    }
    private static double ms(long nanos){
        return nanos / 1_000_000.0;
    }

}
