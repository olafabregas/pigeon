package io.pigeon.core.order;

import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class OrderControllerTest {
    private final OrderService service = mock(OrderService.class);
    private final OrderController controller = new OrderController(service);

    @Test
    void createsAnOrder() {
        Order order = new Order(UUID.randomUUID(), "customer", new BigDecimal("12.50"),
                "CREATED", OffsetDateTime.now());
        when(service.create(new CreateOrderRequest("customer", new BigDecimal("12.5")))).thenReturn(order);

        var response = controller.create(new CreateOrderRequest("customer", new BigDecimal("12.5")));

        assertThat(response.getStatusCode().value()).isEqualTo(201);
        assertThat(response.getBody()).isSameAs(order);
        verify(service).create(new CreateOrderRequest("customer", new BigDecimal("12.5")));
    }

    @Test
    void rejectsMissingCustomerAndNegativeAmount() {
        assertThatThrownBy(() -> controller.create(new CreateOrderRequest("", BigDecimal.ONE)))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(ex -> assertThat(((ResponseStatusException) ex).getStatusCode().value()).isEqualTo(400));
        assertThatThrownBy(() -> controller.create(new CreateOrderRequest("customer", new BigDecimal("-1"))))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(ex -> assertThat(((ResponseStatusException) ex).getStatusCode().value()).isEqualTo(400));
        verifyNoInteractions(service);
    }
}
