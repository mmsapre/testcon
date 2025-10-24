
import com.example.jms.alert.AlertNotifier;
import com.example.jms.metrics.SolaceConnectionMetrics;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Detects runtime JMS errors and triggers retry/failover asynchronously. */
public class SolaceConnectionListener implements ExceptionListener {

    private static final Logger log = LoggerFactory.getLogger(SolaceConnectionListener.class);
    private final MultiHostFailoverConnectionFactory factory;
    private final String host;
    private final SolaceConnectionMetrics metrics;
    private final AlertNotifier notifier;

    public SolaceConnectionListener(MultiHostFailoverConnectionFactory factory,
                                    String host,
                                    SolaceConnectionMetrics metrics,
                                    AlertNotifier notifier) {
        this.factory = factory;
        this.host = host;
        this.metrics = metrics;
        this.notifier = notifier;
    }

    @Override
    public void onException(JMSException ex) {
        log.error("💥 JMS Exception on {}: {}", host, ex.getMessage());
        metrics.markDown(host);
        notifier.notify("JMS connection failed on " + host + ": " + ex.getMessage());

        Thread t = new Thread(() -> factory.retryAndRecover(host), "solace-retry-thread");
        t.setDaemon(true);
        t.setPriority(Thread.MIN_PRIORITY);
        t.start();
    }
}
