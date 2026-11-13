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
    private final Map<String, List<Double>> gauges = new ConcurrentHashMap<>();
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
        for(String name: gauges.keySet()){
            List<Double> values = new ArrayList<>(gauges.get(name));
            double max = Collections.max(values);
            System.out.println(name + " ("+values.size()+" samples) max=" + max);
        }

    }
    private static long percentile(List<Long> sorted, double p){
        int position = (int) Math.ceil(p / 100.0 * sorted.size());
        return sorted.get(Math.max(1, position) - 1);
    }
    public double percentileMs(String signal, double p){
        List<Long> list = samples.get(signal);
        if(list == null || list.isEmpty()) return Double.NaN;
        List<Long> sorted = new ArrayList<>(list);
        Collections.sort(sorted);
        return ms(percentile(sorted, p));
    }
    public long countOf(String key){
        LongAdder a = counts.get(key);
        return a == null ? 0 : a.sum();
    }
    public Map<String, Long> countsStartingWith(String prefix){
        Map<String, Long> found = new HashMap<>();
        counts.forEach((k,v)->{
            if (k.startsWith(prefix)) found.put(k, v.sum());
        });
        return found;
    }
    private static double ms(long nanos){
        return nanos / 1_000_000.0;
    }
    public double gaugeMean(String name){
        List<Double> copy = new ArrayList<>(gauges.getOrDefault(name, List.of()));
        if(copy.isEmpty()) return Double.NaN;
        double sum = 0;
        for(double v:copy) sum+=v;
        return sum/copy.size();
    }
    public double gaugePercentile(String name, double p){
        List<Double> sorted = new ArrayList<>(gauges.getOrDefault(name, List.of()));
        if(sorted.isEmpty()) return Double.NaN;
        Collections.sort(sorted);
        return sorted.get(Math.max(1, (int) Math.ceil(p / 100.0 * sorted.size())) - 1);

    }
    public List<String> gaugeNamesEndingWith(String suffix){
        return gauges.keySet().stream().filter(k -> k.endsWith(suffix)).toList();
    }
    public void gauge(String name, double value){
        gauges.computeIfAbsent(name, k ->Collections.synchronizedList(new ArrayList<>())).add(value);
    }
    public void reset() {
        samples.clear();
        gauges.clear();
        counts.keySet().removeIf(k -> !(k.startsWith("error:") || k.startsWith("timeout:")));
    }


}
