package com.example.loadTestBilla.loadTest;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static List<String> baseUrls = List.of(
            System.getProperty("load.base-urls", "http://localhost:8081,http://localhost:8082").split(","));;

    public static final ObjectMapper JSON = new ObjectMapper();
    public static final Metrics metrics = new Metrics();
    private static final RequestTracker httpTracker = new RequestTracker("HTTP", metrics);

    record Account(String id, String token) {}
    static final Path ACCOUNTS_FILE = Path.of("target/load/accounts.csv");
    static final ConcurrentLinkedQueue<Account> pool = new ConcurrentLinkedQueue<>();
    static final ConcurrentHashMap<String, Set<String>> matchServers = new ConcurrentHashMap<>();
    static final ConcurrentHashMap<String, Long> repAcceptedAt = new ConcurrentHashMap<>();
    static final ConcurrentHashMap<String, Long> opponentRepAt = new ConcurrentHashMap<>();


    static volatile boolean measuring = true;
    static int concurrency = Integer.getInteger("load.concurrency", 20);
    static int warmupSeconds = Integer.getInteger("load.warmup", 40);
    static int steadySeconds = Integer.getInteger("load.steady", 60);
    static long seed = Long.getLong("load.seed", 42L);
    static Set<String> sessionList = ConcurrentHashMap.newKeySet();



    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.printf("Settings: concurrency=%d, warmupsec=%d, steadySec=%d, seed=%d\n",concurrency, warmupSeconds, steadySeconds, seed);

        double rate = concurrency / 9.0 /2;
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

        ServerScraper scraper = new ServerScraper(http, baseUrls, metrics);
        scheduler.scheduleAtFixedRate(scraper::scrapeOnce, 1, 1, TimeUnit.SECONDS);


        int needed = 2 * concurrency;
        List<Account> accounts = loadAccounts();
        if(accounts.size() < needed || !tokenWorks(http, baseUrls.get(0), accounts.get(0).token())){
            System.out.println("Provisioning " + needed + " accounts...");
            accounts = provisionAccounts(http, needed);
            saveAccount(accounts);
        }
        pool.addAll(accounts);
        metrics.reset();
        long start = System.nanoTime();
        List<CompletableFuture<Void>> sessions = new CopyOnWriteArrayList<>();
        AtomicInteger pairCounter = new AtomicInteger();
        Random serverPicker = new Random(seed + 1);

        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        scheduler.scheduleAtFixedRate(()->{
           if(!measuring) return;
           metrics.gauge("Active Sessions", sessionList.size());
           metrics.gauge("GENERATOR_CPU_PERCENT", os.getProcessCpuLoad()*100);
        }, 1 , 1, TimeUnit.SECONDS);

        scheduler.schedule(()-> {
                    metrics.reset();
                    scraper.resetCounters();
                }
            , warmupSeconds, TimeUnit.SECONDS);

        for(long offset : arrivals){
            long intended = start + offset;
            scheduler.schedule(() -> {
                metrics.record("GENERATOR_LAG", System.nanoTime() - intended);
                int n = pairCounter.incrementAndGet();

                Account a1 = pool.poll();
                Account a2 = pool.poll();
                if(a1 == null || a2 == null){
                    if(a1!= null) pool.offer(a1);
                    if (a2!= null) pool.offer(a2);
                    metrics.count("error:pool-empty");
                    return;
                }
                for(Account account : List.of(a1, a2)){
                    String name = "P-" + n + (account == a1 ? "-a" : "-b");
                    String baseUrl = baseUrls.get(serverPicker.nextInt(baseUrls.size()));
                    System.out.println(name + " -> " + baseUrl);
                    PlayerSession session = PlayerSession.builder()
                            .http(http)
                            .baseUrl(baseUrl)
                            .scheduler(scheduler)
                            .metrics(metrics)
                            .name(name)
                            .token(account.token())
                            .listener(new Listener(name, metrics, scheduler, baseUrl))
                            .build();
                    CompletableFuture<Void> f = session.start();
                    sessions.add(f);
                    sessionList.add(session.getName());
                    f.whenComplete((v, error) ->{
                        sessionList.remove(session.getName());
                        scheduler.schedule(()-> pool.offer(account), 1, TimeUnit.SECONDS);
                    });

                }
            }, intended - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        long endNanos = start + (warmupSeconds+steadySeconds)*1_000_000_000L;
        long sleepMs = (endNanos - System.nanoTime()) / 1_000_000;
        if(sleepMs>0) Thread.sleep(sleepMs);
        measuring = false;
        try{
            CompletableFuture.allOf(sessions.toArray(new CompletableFuture[0]))
                    .orTimeout(60, TimeUnit.SECONDS)
                    .join();
        } catch (Exception e){
            System.out.println("Unfinished sessions: " + sessions.stream().filter(f -> !f.isDone()).count());
            System.out.println("Some sessions failed or timed out; "+e);
        }
        writeReport(scraper, sessions.size());
        executor.shutdown();
        scheduler.shutdown();
        System.exit(0);
    }

    static List<Account> provisionAccounts(HttpClient http, int count){
        Semaphore inFlight = new Semaphore(20);
        List<CompletableFuture<Account>> futures = new ArrayList<>();
        for(int i = 1; i<=count; i++){
            inFlight.acquireUninterruptibly();
            futures.add(register(http, baseUrls.get(0),"acc-"+i)
                    .thenApply(r-> new Account(r.get("id").asString(), r.get("token").asString()))
                    .whenComplete((a, error) -> inFlight.release()));
        }
        return futures.stream().map(CompletableFuture::join).toList();
    }
    static void saveAccount(List<Account> accounts) throws IOException {
        Files.createDirectories(ACCOUNTS_FILE.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("id,token");
        for(Account a : accounts) lines.add(a.id() + "," + a.token());
        Files.write(ACCOUNTS_FILE, lines);
    }
    static List<Account> loadAccounts() throws IOException {
        if(!Files.exists(ACCOUNTS_FILE)) return List.of();
        List<Account> result = new ArrayList<>();
        for(String line: Files.readAllLines(ACCOUNTS_FILE).stream().skip(1).toList()){
            String[] parts = line.split(",",2);
            result.add(new Account(parts[0], parts[1]));
        }
        return result;
    }
    static boolean tokenWorks(HttpClient http, String baseUrl, String token){
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/api/players/me"))
                .header("Authorization", "Bearer " + token)
                .GET().build();
        return http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(r -> r.statusCode() == 200)
                .exceptionally(error -> false)
                .join();
    }

    static CompletableFuture<JsonNode> register(HttpClient http, String baseUrl, String name) {
        String body = "{\"name\":\"" + name + "\"}";
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/api/players"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return send(http, request, 201, "REGISTER", body);
    }

    static CompletableFuture<JsonNode> joinQueue(HttpClient http, String baseUrl, Listener listener, String token) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/api/queue"))
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return send(http, request, 202, "QUEUE_ACCEPTED", null).thenApply(reply -> {
            listener.startQueueTimer();
            return reply;
        });
    }

    static CompletableFuture<WebSocket> connect(HttpClient http,String baseUrl, Listener listener, String token) {
        String wsUrl = baseUrl.replaceFirst("^http", "ws");
        long start = System.nanoTime();
        return http.newWebSocketBuilder()
                .buildAsync(URI.create(wsUrl + "/ws?token=" + token), listener)
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
    static void writeReport(ServerScraper scraper, int sessionsStarted) throws IOException {
        var buffer = new java.io.ByteArrayOutputStream();
        var original = System.out;
        System.setOut(new java.io.PrintStream(buffer, true, java.nio.charset.StandardCharsets.UTF_8));
        try {
            System.out.println("# Load test: " + concurrency + " players");
            System.out.println("seed=" + seed + ", warmup=" + warmupSeconds + "s, steady=" + steadySeconds + "s");
            System.out.println("servers=" + baseUrls);
            System.out.println("Sessions started: " + sessionsStarted + ", HTTP unanswered=" + httpTracker.unanswered());
            metrics.printMetrics();
            scraper.printCounterDeltas();
            System.out.println("VERDICT: " + Verdict.evaluate(metrics, scraper, concurrency));
        } finally {
            System.setOut(original);
        }
        String text = buffer.toString(java.nio.charset.StandardCharsets.UTF_8);
        Path file = Path.of("target/load/players-" + concurrency + ".md");
        Files.createDirectories(file.getParent());
        Files.writeString(file, text);
        System.out.println(text);
        System.out.println("Report written to " + file);
    }

}
