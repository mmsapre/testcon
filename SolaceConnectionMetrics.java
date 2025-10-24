
import io.micrometer.core.instrument.*;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Micrometer metrics for Solace JMS connection health. */
@Component
public class SolaceConnectionMetrics {

    private final MeterRegistry registry;
    private final Map<String, AtomicInteger> hostStatus = new ConcurrentHashMap<>();

    public SolaceConnectionMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void markUp(String host) {
        getGauge(host).set(1);
    }

    public void markDown(String host) {
        getGauge(host).set(0);
    }

    public void incFailure(String host) {
        Counter.builder("solace.connection.failures.total")
                .tag("host", host)
                .description("Total connection failures for host")
                .register(registry)
                .increment();
    }

    private AtomicInteger getGauge(String host) {
        return hostStatus.computeIfAbsent(host, h -> {
            AtomicInteger ai = new AtomicInteger(0);
            Gauge.builder("solace.connection.status", ai, AtomicInteger::get)
                    .description("Solace connection status (1=UP,0=DOWN)")
                    .tag("host", host)
                    .register(registry);
            return ai;
        });
    }
}
