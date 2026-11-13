package io.pigeon.core.order;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record Order(UUID id, String customerId, BigDecimal amount, String status, OffsetDateTime createdAt) {
}
