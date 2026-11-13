package io.pigeon.core.order;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<Order> create(@RequestBody CreateOrderRequest req) {
        if (req == null || req.customerId() == null || req.customerId().isBlank()
                || req.amount() == null || req.amount().signum() < 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "customerId and non-negative amount required");
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(service.create(req));
    }

    @GetMapping("/{id}")
    public Order get(@PathVariable UUID id) {
        return service.find(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }
}
