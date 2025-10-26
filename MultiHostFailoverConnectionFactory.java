
import com.example.jms.alert.AlertNotifier;
import com.example.jms.metrics.SolaceConnectionMetrics;
import com.example.jms.support.ConnectionResetHandler;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import jakarta.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Multi-host connection factory for Solace JMS with retry + runtime failover.
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

    private ConnectionResetHandler resetHandler;
    private final AtomicInteger activeIndex = new AtomicInteger(-1);

    public MultiHostFailoverConnectionFactory(
            String hostsCsv,
            String vpn,
            String username,
            String password,
            SolaceConnectionMetrics metrics,
            AlertNotifier notifier,
            int retryLimit,
            long retryDelayMs) {

        this.hosts = Arrays.stream(hostsCsv.split("\\s*,\\s*"))
                .filter(h -> !h.isBlank())
                .distinct()
                .collect(Collectors.toList());
        if (hosts.isEmpty()) throw new IllegalArgumentException("No Solace hosts configured!");

        this.vpn = vpn;
        this.username = username;
        this.password = password;
        this.metrics = metrics;
        this.notifier = notifier;
        this.retryLimit = retryLimit;
        this.retryDelayMs = retryDelayMs;
    }

    public void setResetHandler(ConnectionResetHandler resetHandler) {
        this.resetHandler = resetHandler;
    }

    // ---------------------------------------------------------------------
    // JMS methods
    // ---------------------------------------------------------------------
    @Override
    public Connection createConnection() throws JMSException {
        return createConnection(username, password);
    }

    @Override
    public Connection createConnection(String user, String pass) throws JMSException {
        return connectInternal(user, pass);
    }

    @Override
    public JMSContext createContext() {
        return createContext(username, password, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String user, String pass) {
        return createContext(user, pass, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String user, String pass, int sessionMode) {
        return connectContextInternal(user, pass, sessionMode);
    }

    // ---------------------------------------------------------------------
    // Core retry + failover logic
    // ---------------------------------------------------------------------
    private Connection connectInternal(String user, String pass) throws JMSException {
        int start = (activeIndex.get() >= 0) ? activeIndex.get() : 0;

        for (int i = 0; i < hosts.size(); i++) {
            int idx = (start + i) % hosts.size();
            String host = hosts.get(idx);

            for (int attempt = 1; attempt <= retryLimit; attempt++) {
                try {
                    log.info("🔌 Connecting to Solace [{}] attempt {}/{}", host, attempt, retryLimit);
                    SolConnectionFactory f = SolJmsUtility.createConnectionFactory();
                    f.setHost(host);
                    f.setVPN(vpn);
                    f.setUsername(user);
                    f.setPassword(pass);
                    f.setReconnectRetries(0);

                    Connection conn = f.createConnection();
                    conn.setExceptionListener(new SolaceConnectionListener(this, host, metrics, notifier));
                    activeIndex.set(idx);

                    metrics.markUp(host);
                    log.info("✅ Connected to host={} vpn={}", host, vpn);
                    return conn;

                } catch (Exception e) {
                    metrics.markDown(host);
                    metrics.incFailure(host);
                    log.warn("❌ Attempt {}/{} failed for host {}: {}", attempt, retryLimit, host, e.getMessage());
                    sleep();
                }
            }

            notifier.notify("All retries failed for host " + host);
            switchToNextHost();
        }

        throw new JMSException("All Solace hosts failed: " + hosts);
    }

    private JMSContext connectContextInternal(String user, String pass, int sessionMode) {
        try {
            return connectInternal(user, pass).createSession(false, sessionMode).getJMSContext();
        } catch (JMSException e) {
            throw new RuntimeException("Failed to create JMSContext", e);
        }
    }

    private void sleep() {
        try { Thread.sleep(retryDelayMs); } catch (InterruptedException ignored) {}
    }

    // ---------------------------------------------------------------------
    // Failover handling
    // ---------------------------------------------------------------------
    public synchronized void retryAndRecover(String failingHost) {
        log.warn("⚠️ Connection lost for {} → retrying {} times", failingHost, retryLimit);

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
                    log.info("✅ Reconnected to {}", failingHost);
                    return;
                }

            } catch (Exception e) {
                metrics.markDown(failingHost);
                metrics.incFailure(failingHost);
                log.warn("❌ Retry {}/{} failed for {}: {}", attempt, retryLimit, failingHost, e.getMessage());
                sleep();
            }
        }

        notifier.notify("Permanent failure on host " + failingHost);
        switchToNextHost();
    }

    public synchronized void switchToNextHost() {
        int current = activeIndex.get();
        int next = (current + 1) % hosts.size();
        if (next == current) return;

        log.warn("🔄 Switching host {} → {}", hosts.get(current), hosts.get(next));
        activeIndex.set(next);

        if (resetHandler != null) {
            log.info("🔁 Triggering external connection reset via handler...");
            resetHandler.resetConnection();
        } else {
            log.warn("⚠️ No reset handler set; cannot reset external cache");
        }
    }

    public String getActiveHost() {
        int idx = activeIndex.get();
        return (idx >= 0 && idx < hosts.size()) ? hosts.get(idx) : "none";
    }
}
