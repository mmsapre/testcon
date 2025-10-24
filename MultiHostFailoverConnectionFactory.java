
import com.example.jms.alert.AlertNotifier;
import com.example.jms.metrics.SolaceConnectionMetrics;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import jakarta.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Solace Multi-Host ConnectionFactory:
 * - Retries 5x before switching to secondary
 * - Logs and alerts failures
 * - Updates Micrometer metrics
 * - Compatible with Camel JMS
 */
public class MultiHostFailoverConnectionFactory implements ConnectionFactory {

    private static final Logger log = LoggerFactory.getLogger(MultiHostFailoverConnectionFactory.class);

    private final List<String> hosts;
    private final String vpn;
    private final String username;
    private final String password;
    private final int retryLimit;
    private final long retryDelayMs;

    private final SolaceConnectionMetrics metrics;
    private final AlertNotifier notifier;
    private final CachingConnectionFactory cachingFactory;

    private final AtomicInteger activeIndex = new AtomicInteger(-1);
    private final AtomicInteger failureCount = new AtomicInteger(0);

    public MultiHostFailoverConnectionFactory(
            String hostsCsv,
            String vpn,
            String username,
            String password,
            SolaceConnectionMetrics metrics,
            AlertNotifier notifier,
            CachingConnectionFactory cachingFactory,
            int retryLimit,
            long retryDelayMs
    ) {
        this.hosts = parseHosts(hostsCsv);
        this.vpn = vpn;
        this.username = username;
        this.password = password;
        this.metrics = metrics;
        this.notifier = notifier;
        this.cachingFactory = cachingFactory;
        this.retryLimit = retryLimit;
        this.retryDelayMs = retryDelayMs;

        if (hosts.isEmpty()) {
            throw new IllegalStateException("No Solace hosts configured!");
        }
    }

    private static List<String> parseHosts(String csv) {
        return Arrays.stream(csv.split("\\s*,\\s*"))
                .filter(s -> !s.isBlank())
                .distinct()
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------
    // JMS 1.1 API
    // ---------------------------------------------------------------------

    @Override
    public Connection createConnection() {
        return createConnection(username, password);
    }

    @Override
    public Connection createConnection(String username, String password) {
        int startingIndex = activeIndex.get() >= 0 ? activeIndex.get() : 0;

        for (int i = 0; i < hosts.size(); i++) {
            int hostIndex = (startingIndex + i) % hosts.size();
            String host = hosts.get(hostIndex);

            boolean connected = false;
            for (int attempt = 1; attempt <= retryLimit; attempt++) {
                try {
                    SolConnectionFactory f = SolJmsUtility.createConnectionFactory();
                    f.setHost(host);
                    f.setVPN(vpn);
                    f.setUsername(username);
                    f.setPassword(password);
                    f.setReconnectRetries(0);

                    Connection c = f.createConnection();
                    activeIndex.set(hostIndex);
                    failureCount.set(0);
                    metrics.markUp();
                    log.info("✅ Connected to Solace host: {} (VPN={})", host, vpn);
                    return c;
                } catch (Exception e) {
                    metrics.markDown();
                    metrics.incFailure();
                    log.warn("❌ [Host={}] Connection attempt {}/{} failed: {}", host, attempt, retryLimit, e.getMessage());

                    try { Thread.sleep(retryDelayMs); } catch (InterruptedException ignored) {}
                }
            }

            // all retries failed for current host
            notifier.notify("Solace connection failed after " + retryLimit + " retries on host " + host);
            log.error("🚨 All {} retry attempts failed for Solace host: {}", retryLimit, host);

            // switch to next host automatically
            if (i < hosts.size() - 1) {
                log.warn("🔁 Switching to secondary host: {}", hosts.get((hostIndex + 1) % hosts.size()));
            }
        }

        throw new RuntimeException("❌ All Solace hosts failed to connect: " + hosts);
    }

    // ---------------------------------------------------------------------
    // JMS 2.0 Context
    // ---------------------------------------------------------------------
    @Override
    public JMSContext createContext() {
        return createContext(username, password);
    }

    @Override
    public JMSContext createContext(String username, String password) {
        Connection c = createConnection(username, password);
        try {
            return c.createSession(Session.AUTO_ACKNOWLEDGE).getJMSContext();
        } catch (JMSException e) {
            throw new RuntimeException("Failed to create JMSContext", e);
        }
    }

    @Override
    public JMSContext createContext(String username, String password, int sessionMode) {
        Connection c = createConnection(username, password);
        try {
            return c.createSession(sessionMode).getJMSContext();
        } catch (JMSException e) {
            throw new RuntimeException("Failed to create JMSContext", e);
        }
    }

    // ---------------------------------------------------------------------
    // Failover Handling
    // ---------------------------------------------------------------------
    public void handleListenerFailure(Exception ex) {
        int failures = failureCount.incrementAndGet();
        metrics.markDown();
        log.warn("⚠️ Listener failure detected: {}", ex.getMessage());
        if (failures >= retryLimit) {
            notifier.notify("Listener repeated failure detected for host " + getActiveHost());
            switchToNextHost();
        }
    }

    private synchronized void switchToNextHost() {
        int current = activeIndex.get();
        int next = (current + 1) % hosts.size();
        if (next == current) return;
        log.warn("🔄 Switching Solace host {} → {}", hosts.get(current), hosts.get(next));
        activeIndex.set(next);
        failureCount.set(0);
        cachingFactory.resetConnection();
    }

    // ---------------------------------------------------------------------
    // Accessors
    // ---------------------------------------------------------------------
    public String getActiveHost() {
        int idx = activeIndex.get();
        return (idx >= 0 && idx < hosts.size()) ? hosts.get(idx) : "none";
    }
}
