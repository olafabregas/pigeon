package io.pigeon.core.outbox;

import io.pigeon.outbox.Outbox;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.util.Map;
import java.util.UUID;

/** Demo admin endpoints to requeue DEAD outbox events after a broker outage. */
@RestController
@ConditionalOnProperty(name = "pigeon.demo.enabled", havingValue = "true")
@RequestMapping("/api/outbox")
public class OutboxAdminController {
    private final Outbox outbox;

    public OutboxAdminController(Outbox outbox) {
        this.outbox = outbox;
    }

    @PostMapping("/dead/{id}/replay")
    public Map<String, Object> replayOne(@PathVariable UUID id) {
        if (!outbox.replayDead(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no DEAD event with id " + id);
        }
        return Map.of("replayed", 1, "id", id.toString());
    }

    @PostMapping("/dead/replay-all")
    public Map<String, Object> replayAll() {
        int n = outbox.replayAllDead();
        return Map.of("replayed", n);
    }
}
