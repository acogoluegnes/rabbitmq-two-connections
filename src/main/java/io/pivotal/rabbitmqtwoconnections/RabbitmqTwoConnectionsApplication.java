package io.pivotal.rabbitmqtwoconnections;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Example of a Spring Boot application that uses 2 different connections.
 * {@link RabbitAutoConfiguration} is disabled and beans are declared explicitly.
 * Beans for the first connection are injected by default because
 * they are declared with {@link Primary} annotation.
 * To autowire beans for the second connection, add
 * <code>@Qualifier("secondary")</code>.
 * The second connection factory is configured in the default
 * Spring Boot configuration file with the <code>spring.rabbitmq2</code>
 * prefix.
 */
@SpringBootApplication(exclude = RabbitAutoConfiguration.class)
public class RabbitmqTwoConnectionsApplication {

    private final static String QUEUE = "queue";

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext ctx = SpringApplication.run(RabbitmqTwoConnectionsApplication.class, args);
        RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
        RabbitTemplate rabbitOperationsSecondary = ctx.getBean("rabbitOperationsSecondary", RabbitTemplate.class);
        while (true) {
            Thread.sleep(100L);
            template.convertAndSend(QUEUE, "Hello, world!");
            rabbitOperationsSecondary.convertAndSend(QUEUE, "Hello from secondary!");
        }
    }

    @Bean
    public SimpleMessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(QUEUE);
        container.setMessageListener(exampleListener());
        return container;
    }

    @Bean
    public MessageListener exampleListener() {
        return message -> {
            //System.out.println("received: " + message);
        };
    }

    @Bean
    public Queue metricsQueue() {
        return new Queue(QUEUE);
    }

    /* first connection */

    @Bean
    @ConfigurationProperties(prefix = "spring.rabbitmq")
    @Primary
    public RabbitProperties rabbitProperties() {
        return new RabbitProperties();
    }

    @Bean
    @Primary
    public ConnectionFactory connectionFactory(RabbitProperties rabbitProperties) {
        CachingConnectionFactory connectionFactory = createConnectionFactory(rabbitProperties);
        AtomicLong counter = new AtomicLong(0);
        connectionFactory.setConnectionNameStrategy(cf -> "connection-1-" + counter.incrementAndGet());
        return connectionFactory;
    }

    @Bean
    @Primary
    public RabbitOperations rabbitOperations(ConnectionFactory connectionFactory) {
        return new RabbitTemplate(connectionFactory);
    }

    @Bean
    @Primary
    public AmqpAdmin amqpAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }

    /* second connection */

    @Bean
    @ConfigurationProperties(prefix = "spring.rabbitmq2")
    @Qualifier("secondary")
    public RabbitProperties rabbitPropertiesSecondary() {
        return new RabbitProperties();
    }

    @Bean
    @Qualifier("secondary")
    public ConnectionFactory connectionFactorySecondary(
        @Qualifier("secondary")
            RabbitProperties rabbitPropertiesSecondary) {
        CachingConnectionFactory connectionFactory = createConnectionFactory(rabbitPropertiesSecondary);
        AtomicLong counter = new AtomicLong(0);
        connectionFactory.setConnectionNameStrategy(cf -> "connection-2-" + counter.incrementAndGet());
        return connectionFactory;
    }

    @Bean
    public RabbitOperations rabbitOperationsSecondary(
        @Qualifier("secondary")
            ConnectionFactory connectionFactorySecondary) {
        return new RabbitTemplate(connectionFactorySecondary);
    }

    private CachingConnectionFactory createConnectionFactory(RabbitProperties rabbitProperties) {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost(rabbitProperties.getHost());
        connectionFactory.setPort(rabbitProperties.getPort());
        connectionFactory.setUsername(rabbitProperties.getUsername());
        connectionFactory.setPassword(rabbitProperties.getPassword());
        return connectionFactory;
    }
}
