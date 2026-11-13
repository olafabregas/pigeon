package io.pigeon.core.order;

import java.math.BigDecimal;

public record CreateOrderRequest(String customerId, BigDecimal amount) {
}
