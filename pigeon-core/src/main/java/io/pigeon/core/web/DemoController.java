package io.pigeon.core.web;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api")
public class DemoController {

    @GetMapping("/ping")
    public String ping() {
        return "pong";
    }

    @GetMapping("/slow")
    public String slow(@RequestParam(defaultValue = "2000") long ms) throws InterruptedException {
        Thread.sleep(Math.min(Math.max(ms, 0), 60_000));
        return "slow " + ms + "ms";
    }

    @GetMapping("/fail")
    public String fail() {
        throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "intentional failure");
    }
}
