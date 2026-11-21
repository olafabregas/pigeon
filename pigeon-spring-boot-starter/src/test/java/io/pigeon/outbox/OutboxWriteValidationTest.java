package io.pigeon.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxWriteValidationTest {

    private final OutboxRepository repository = mock(OutboxRepository.class);
    private final Outbox outbox = new Outbox(repository, new ObjectMapper());

    @BeforeEach
    void startTransaction() {
        TransactionSynchronizationManager.setActualTransactionActive(true);
    }

    @AfterEach
    void endTransaction() {
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "   ", "not json", "{", "{\"a\":}", "{} trailing", "{}{}", "[1,2", "'single'"})
    void invalidJsonIsRejectedBeforeAnythingIsWritten(String payload) {
        assertThatThrownBy(() -> outbox.write("order", "1", "order.created", payload))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outbox payloadJson");

        verify(repository, never()).insert(anyString(), anyString(), anyString(), anyString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"orderId\":\"1\",\"amount\":10.5}", "[]", "[1,2,3]",
            "\"plain text\"", "123", "true", "null", "  {\"padded\": true}  "})
    void validJsonIsStoredExactlyAsGiven(String payload) {
        UUID id = UUID.randomUUID();
        when(repository.insert("order", "1", "order.created", payload)).thenReturn(id);

        assertThat(outbox.write("order", "1", "order.created", payload)).isEqualTo(id);
    }

    @Test
    void errorMessageDoesNotEchoTheRejectedPayload() {
        String secret = "{\"cardNumber\":\"4111111111111111\"";

        assertThatThrownBy(() -> outbox.write("order", "1", "order.created", secret))
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("4111").doesNotContain("cardNumber"));
    }

    @Test
    void errorMessageTellsTheCallerHowToFixIt() {
        assertThatThrownBy(() -> outbox.write("order", "1", "order.created", "hello"))
                .hasMessageContaining("pass the object instead of a String");
    }

    @Test
    void objectPayloadIsSerializedByJackson() {
        when(repository.insert(anyString(), anyString(), anyString(), anyString())).thenReturn(UUID.randomUUID());

        outbox.write("order", "1", "order.created", Map.of("orderId", "1"));

        verify(repository).insert("order", "1", "order.created", "{\"orderId\":\"1\"}");
    }

    @Test
    void stringTypedAsObjectIsSerializedAsAJsonString() {
        when(repository.insert(anyString(), anyString(), anyString(), anyString())).thenReturn(UUID.randomUUID());
        Object text = "hello";

        outbox.write("order", "1", "order.created", text);

        verify(repository).insert("order", "1", "order.created", "\"hello\"");
    }

    @Test
    void unserializableObjectIsRejectedBeforeAnythingIsWritten() {
        Object selfReferencing = new Object();

        assertThatThrownBy(() -> outbox.write("order", "1", "order.created", selfReferencing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failed to serialize outbox payload");

        verify(repository, never()).insert(anyString(), anyString(), anyString(), any());
    }

    @Test
    void transactionIsRequiredForBothOverloads() {
        TransactionSynchronizationManager.setActualTransactionActive(false);

        assertThatThrownBy(() -> outbox.write("order", "1", "order.created", "{}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("active database transaction");
        assertThatThrownBy(() -> outbox.write("order", "1", "order.created", Map.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("active database transaction");
    }

    @Test
    void missingTransactionIsReportedEvenWhenThePayloadIsAlsoInvalid() {
        TransactionSynchronizationManager.setActualTransactionActive(false);

        assertThatThrownBy(() -> outbox.write("order", "1", "order.created", "not json"))
                .isInstanceOf(IllegalStateException.class);
    }
}
