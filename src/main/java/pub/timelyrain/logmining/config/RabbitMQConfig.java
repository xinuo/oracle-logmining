package pub.timelyrain.logmining.config;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Configuration
public class RabbitMQConfig {
    private static Logger log = LogManager.getLogger(RabbitMQConfig.class);
    @Autowired
    private Env env;

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory factory) {
        log.info("创建需要的交换机和队列");
        createExchangeAndQueue(factory);

        log.info("应用 Jackson2JsonMessageConverter 序列化 MQ对象");
        RabbitTemplate rabbitTemplate = new RabbitTemplate(factory);
        ObjectMapper om = new ObjectMapper();
        om.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter(om));
        return rabbitTemplate;
    }

    private void createExchangeAndQueue(ConnectionFactory factory) {
        RabbitAdmin admin = new RabbitAdmin(factory);
        FanoutExchange fanoutExchange = new FanoutExchange(env.getExchangeName());
        admin.declareExchange(fanoutExchange);
        String[] queues = env.getQueueName().split(",");
        for (String q : queues) {
            Queue queue = new Queue(q);
            admin.declareQueue(queue);
            admin.declareBinding(new Binding(q, Binding.DestinationType.QUEUE, env.getExchangeName(), "", null));
        }
    }

    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        HashMap m = new HashMap();
        m.put("aa", null);
        m.put("bb", "bb");
        System.out.println(om.writeValueAsString(m));
    }
}
