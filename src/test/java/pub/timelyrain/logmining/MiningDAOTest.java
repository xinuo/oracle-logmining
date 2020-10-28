package pub.timelyrain.logmining;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pub.timelyrain.logmining.biz.MiningDAO;

@SpringBootTest
class MiningDAOTest {
    @Autowired
    private MiningDAO miningDAO;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Test
    void startMining() {
        miningDAO.startMining();
    }
    @Test
    void sendMq(){
        rabbitTemplate.convertAndSend("topic","aa","body");
    }
}