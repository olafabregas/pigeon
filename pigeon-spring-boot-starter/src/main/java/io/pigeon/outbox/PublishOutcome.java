package io.pigeon.outbox;

/**
 * Result of attempting to publish one outbox event.
 * <ul>
 *   <li>{@link #CONFIRMED} — broker ack; mark SENT</li>
 *   <li>{@link #TRANSIENT} — broker down / timeout; soft retry without burning max-attempts</li>
 *   <li>{@link #PERMANENT} — nack or unroutable; counts toward DEAD</li>
 * </ul>
 */
public enum PublishOutcome {
    CONFIRMED,
    TRANSIENT,
    PERMANENT
}
