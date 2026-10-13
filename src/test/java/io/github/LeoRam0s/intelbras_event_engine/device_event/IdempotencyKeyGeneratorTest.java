package io.github.LeoRam0s.intelbras_event_engine.device_event;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotencyKeyGeneratorTest {

    @Test
    void generatesStableSha256FromExactBodyText() {
        String body = "{\"did\":\"device-1\",\"content\":{\"value\":\"a,b\"}}";

        assertThat(IdempotencyKeyGenerator.generate(body))
                .isEqualTo(
                        "d95494aa9d1df37f1d9e654a3a9495f1ca91bc5391d8b30eb7510b9c40165fa5"
                );
    }

    @Test
    void generatesDifferentHashWhenBodyTextChanges() {
        String body = "{\"did\":\"device-1\",\"content\":{\"value\":\"a,b\"}}";

        assertThat(IdempotencyKeyGenerator.generate(body + " "))
                .isNotEqualTo(IdempotencyKeyGenerator.generate(body));
    }
}