
import com.example.jms.alert.AlertNotifier;
import com.example.jms.metrics.SolaceConnectionMetrics;
import org.apache.camel.component.jms.JmsComponent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.connection.CachingConnectionFactory;

/** Camel JMS configuration using resilient Solace connection factory. */
@Configuration
public class JmsConfig {

    @Bean
    public CachingConnectionFactory cachingConnectionFactory() {
        CachingConnectionFactory cf = new CachingConnectionFactory();
        cf.setSessionCacheSize(10);
        cf.setReconnectOnException(true);
        cf.setCacheConsumers(false);
        return cf;
    }

    @Bean
    public MultiHostFailoverConnectionFactory solaceFactory(
            SolaceConnectionMetrics metrics,
            AlertNotifier notifier,
            CachingConnectionFactory cachingFactory,
            @Value("${solace.host}") String hosts,
            @Value("${solace.vpn}") String vpn,
            @Value("${solace.username}") String user,
            @Value("${solace.password}") String pass
    ) {
        return new MultiHostFailoverConnectionFactory(
                hosts, vpn, user, pass, metrics, notifier, cachingFactory, 5, 5000
        );
    }

    @Bean
    public JmsComponent jms(MultiHostFailoverConnectionFactory solaceFactory,
                            CachingConnectionFactory cachingFactory) {
        cachingFactory.setTargetConnectionFactory(solaceFactory);
        cachingFactory.setReconnectOnException(true);
        return JmsComponent.jmsComponent(cachingFactory);
    }
}
