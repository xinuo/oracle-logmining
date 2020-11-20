package pub.timelyrain.logmining.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import pub.timelyrain.logmining.biz.CounterService;
import pub.timelyrain.logmining.pojo.Counter;

import java.util.List;

@RestController
public class StatusController {

    private final CounterService counterService;

    @Autowired
    public StatusController(CounterService counterService) {
        this.counterService = counterService;
    }

    @GetMapping("/status")
    public List<Counter> status() {
        return counterService.traceList();
    }
}
