package com.example.jms.config;

import com.example.jms.alert.AlertNotifier;
import com.example.jms.metrics.SolaceConnectionMetrics;
import com.example.jms.support.ConnectionResetHandler;
import org.apache.camel.component.jms.JmsComponent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.connection.CachingConnectionFactory;

@Configuration
public class JmsConfig {

    @Bean
    public SolaceConnectionMetrics solaceMetrics() {
        return new SolaceConnectionMetrics();
    }

    @Bean
    public AlertNotifier alertNotifier() {
        return new AlertNotifier();
    }

    @Bean
    public MultiHostFailoverConnectionFactory solaceConnectionFactory(
            SolaceConnectionMetrics metrics,
            AlertNotifier notifier,
            @Value("${solace.host}") String hosts,
            @Value("${solace.vpn}") String vpn,
            @Value("${solace.username}") String user,
            @Value("${solace.password}") String pass) {

        return new MultiHostFailoverConnectionFactory(
                hosts, vpn, user, pass,
                metrics, notifier,
                5, 3000   // retry 5 times, 3s delay
        );
    }

    @Bean
    public CachingConnectionFactory cachingConnectionFactory(
            MultiHostFailoverConnectionFactory multiHostFactory) {

        CachingConnectionFactory caching = new CachingConnectionFactory();
        caching.setTargetConnectionFactory(multiHostFactory);
        caching.setSessionCacheSize(10);
        caching.setReconnectOnException(true);

        // Bind callback (no circular dependency)
        ConnectionResetHandler handler = caching::resetConnection;
        multiHostFactory.setResetHandler(handler);

        return caching;
    }

    @Bean(name = "jms")
    public JmsComponent jms(CachingConnectionFactory cachingConnectionFactory) {
        return JmsComponent.jmsComponent(cachingConnectionFactory);
    }
}
