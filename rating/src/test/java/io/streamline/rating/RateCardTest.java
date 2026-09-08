package io.streamline.rating;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class RateCardTest {

    private static RateCard card(String unitPrice, String includedUnits) {
        return new RateCard("api_calls", 3, new BigDecimal(unitPrice), new BigDecimal(includedUnits),
                LocalDate.of(2025, 1, 1));
    }

    @Test
    void chargesOnlyUnitsAboveIncluded() {
        RateCard card = card("0.0025", "1000");

        BigDecimal amount = RateCard.price(card, new BigDecimal("1500"));

        assertThat(amount).isEqualByComparingTo(new BigDecimal("1.25"));
    }

    @Test
    void chargesNothingWhenQuantityIsBelowIncluded() {
        RateCard card = card("0.0025", "1000");

        BigDecimal amount = RateCard.price(card, new BigDecimal("640"));

        assertThat(amount).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void roundsHalfUpToCents() {
        RateCard card = card("0.0033", "0");

        BigDecimal amount = RateCard.price(card, new BigDecimal("1"));

        assertThat(amount).isEqualByComparingTo(new BigDecimal("0.00"));

        BigDecimal larger = RateCard.price(card, new BigDecimal("5"));

        assertThat(larger).isEqualByComparingTo(new BigDecimal("0.02"));
    }

    @Test
    void zeroQuantityCostsNothing() {
        RateCard card = card("0.0100", "0");

        BigDecimal amount = RateCard.price(card, BigDecimal.ZERO);

        assertThat(amount).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(amount.scale()).isEqualTo(2);
    }

    @Test
    void negativeQuantityIsRejected() {
        RateCard card = card("0.0100", "0");

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> RateCard.price(card, new BigDecimal("-1")));
    }
}
