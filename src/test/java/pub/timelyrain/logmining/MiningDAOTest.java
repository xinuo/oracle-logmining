package pub.timelyrain.logmining;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pub.timelyrain.logmining.biz.MiningDAO;

@SpringBootTest
class MiningDAOTest {
    @Autowired
    private MiningDAO miningDAO;

    @Test
    void startMining() {
        miningDAO.startMining();
    }
}