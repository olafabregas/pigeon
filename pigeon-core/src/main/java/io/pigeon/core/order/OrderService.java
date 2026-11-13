package io.pigeon.core.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pigeon.core.outbox.OutboxRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {
    private final JdbcTemplate jdbc;
    private final OutboxRepository outbox;
    private final ObjectMapper mapper;

    public OrderService(JdbcTemplate jdbc, OutboxRepository outbox, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.mapper = mapper;
    }

    /** Order row and outbox row are written atomically in one transaction. */
    @Transactional
    public Order create(CreateOrderRequest req) {
        UUID id = UUID.randomUUID();
        BigDecimal amount = req.amount().setScale(2, java.math.RoundingMode.HALF_UP);
        jdbc.update("INSERT INTO orders (id, customer_id, amount, status) VALUES (?, ?, ?, 'CREATED')",
                id, req.customerId(), amount);
        Order order = find(id).orElseThrow();
        try {
            String payload = mapper.writeValueAsString(Map.of(
                    "orderId", id.toString(),
                    "customerId", order.customerId(),
                    "amount", order.amount()));
            outbox.insert("order", id.toString(), "order.created", payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
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
