package io.pigeon.core.order;

import io.pigeon.outbox.Outbox;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {
    private final JdbcTemplate jdbc;
    private final Outbox outbox;

    public OrderService(JdbcTemplate jdbc, Outbox outbox) {
        this.jdbc = jdbc;
        this.outbox = outbox;
    }

    /** Order row and outbox row are written atomically in one transaction. */
    @Transactional
    public Order create(CreateOrderRequest req) {
        UUID id = UUID.randomUUID();
        BigDecimal amount = req.amount().setScale(2, java.math.RoundingMode.HALF_UP);
        jdbc.update("INSERT INTO orders (id, customer_id, amount, status) VALUES (?, ?, ?, 'CREATED')",
                id, req.customerId(), amount);
        Order order = find(id).orElseThrow();
        outbox.write("order", id.toString(), "order.created", Map.of(
                "orderId", id.toString(),
                "customerId", order.customerId(),
                "amount", order.amount()));
        return order;
    }

    @Transactional(readOnly = true)
    public Optional<Order> find(UUID id) {
        return jdbc.query("SELECT id, customer_id, amount, status, created_at FROM orders WHERE id = ?",
                (rs, i) -> new Order(
                        rs.getObject("id", UUID.class),
                        rs.getString("customer_id"),
                        rs.getBigDecimal("amount"),
                        rs.getString("status"),
                        rs.getObject("created_at", java.time.OffsetDateTime.class)),
                id).stream().findFirst();
    }
}
