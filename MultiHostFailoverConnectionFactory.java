
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
 * Solace multi-host ConnectionFactory with runtime retry + automatic failover.
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
            throw new IllegalArgumentException("No Solace hosts configured!");
        }
    }

    private static List<String> parseHosts(String csv) {
        return Arrays.stream(csv.split("\\s*,\\s*"))
                .filter(h -> !h.isBlank())
                .distinct()
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------
    // JMS 1.1
    // ---------------------------------------------------------------------
    @Override public Connection createConnection() { return createConnection(username, password); }

    @Override
    public Connection createConnection(String user, String pass) {
        int start = (activeIndex.get() >= 0) ? activeIndex.get() : 0;
        for (int i = 0; i < hosts.size(); i++) {
            int idx = (start + i) % hosts.size();
            String host = hosts.get(idx);

            for (int attempt = 1; attempt <= retryLimit; attempt++) {
                try {
                    log.info("🔌 Trying Solace host [{}] (attempt {}/{})", host, attempt, retryLimit);
                    SolConnectionFactory f = SolJmsUtility.createConnectionFactory();
                    f.setHost(host);
                    f.setVPN(vpn);
                    f.setUsername(user);
                    f.setPassword(pass);
                    f.setReconnectRetries(0);

                    Connection conn = f.createConnection();
                    conn.setExceptionListener(new SolaceConnectionListener(this, host, metrics, notifier));
                    activeIndex.set(idx);
                    failureCount.set(0);
                    metrics.markUp(host);
                    log.info("✅ Connected to Solace host={} vpn={}", host, vpn);
                    return conn;

                } catch (Exception e) {
                    metrics.markDown(host);
                    metrics.incFailure(host);
                    log.warn("❌ Attempt {}/{} failed for host {}: {}", attempt, retryLimit, host, e.getMessage());
                    try { Thread.sleep(retryDelayMs); } catch (InterruptedException ignored) {}
                }
            }

            log.error("🚨 All {} retries failed for host {}, switching.", retryLimit, host);
            notifier.notify("Solace host failed: " + host);
            switchToNextHost();
        }
        throw new RuntimeException("All Solace hosts failed: " + hosts);
    }

    // ---------------------------------------------------------------------
    // JMS 2.0 Context
    // ---------------------------------------------------------------------
    @Override public JMSContext createContext() { return createContext(username, password); }
    @Override public JMSContext createContext(String u, String p) { return createContext(u, p, Session.AUTO_ACKNOWLEDGE); }
    @Override
    public JMSContext createContext(String u, String p, int mode) {
        Connection c = createConnection(u, p);
        try {
            Session s = c.createSession(mode);
            return s.getJMSContext();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create JMSContext", e);
        }
    }

    // ---------------------------------------------------------------------
    // Runtime Retry & Failover
    // ---------------------------------------------------------------------
    public synchronized void retryAndRecover(String failingHost) {
        log.warn("⚠️ Connection lost for {} — retrying...", failingHost);
        for (int attempt = 1; attempt <= retryLimit; attempt++) {
            try {
                log.info("🔁 Reconnect attempt {}/{} for {}", attempt, retryLimit, failingHost);
                SolConnectionFactory f = SolJmsUtility.createConnectionFactory();
                f.setHost(failingHost);
                f.setVPN(vpn);
                f.setUsername(username);
                f.setPassword(password);
                f.setReconnectRetries(0);

                try (Connection conn = f.createConnection()) {
                    metrics.markUp(failingHost);
                    failureCount.set(0);
                    log.info("✅ Reconnected to {}", failingHost);
                    return;
                }

            } catch (Exception e) {
                metrics.markDown(failingHost);
                metrics.incFailure(failingHost);
                log.warn("❌ Retry {}/{} failed for {}: {}", attempt, retryLimit, failingHost, e.getMessage());
                try { Thread.sleep(retryDelayMs); } catch (InterruptedException ignored) {}
            }
        }

        log.error("🚨 All retries failed for {} — switching host.", failingHost);
        notifier.notify("Connection recovery failed for host " + failingHost);
        switchToNextHost();
    }

    public synchronized void switchToNextHost() {
        int current = activeIndex.get();
        int next = (current + 1) % hosts.size();
        if (next == current) return;
        log.warn("🔄 Switching Solace host {} → {}", hosts.get(current), hosts.get(next));
        activeIndex.set(next);
        failureCount.set(0);
        cachingFactory.resetConnection();   // causes Camel/Spring DMLC to reconnect
    }

    public String getActiveHost() {
        int idx = activeIndex.get();
        return (idx >= 0 && idx < hosts.size()) ? hosts.get(idx) : "none";
    }
}
