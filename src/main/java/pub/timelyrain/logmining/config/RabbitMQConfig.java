package pub.timelyrain.logmining.config;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Configuration
public class RabbitMQConfig {
    private static Logger log = LogManager.getLogger(RabbitMQConfig.class);
    @Bean
    public RabbitListenerContainerFactory<?> rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        ObjectMapper om = new ObjectMapper();
        om.setSerializationInclusion(JsonInclude.Include.ALWAYS);

        factory.setMessageConverter(new Jackson2JsonMessageConverter(om));
        return factory;
    }

    @Bean
    public RabbitTemplate rabbitTemplate(CachingConnectionFactory factory) {
        log.info("应用 Jackson2JsonMessageConverter 序列化 MQ对象");
        RabbitTemplate rabbitTemplate = new RabbitTemplate(factory);
        //MessageConverter 为 Jackson2JsonMessageConverter
        ObjectMapper om = new ObjectMapper();
        om.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter(om));
        return rabbitTemplate;
    }



    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        HashMap m = new HashMap();
        m.put("aa",null);
        m.put("bb","bb");
        System.out.println(om.writeValueAsString(m));
    }

}
