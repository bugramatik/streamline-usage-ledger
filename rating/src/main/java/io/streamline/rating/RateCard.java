package io.streamline.rating;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * A versioned price for one meter. A rate card is active from {@code effectiveFrom}
 * until the next version of the same meter takes over.
 */
public record RateCard(
        String meter,
        int version,
        BigDecimal unitPrice,
        BigDecimal includedUnits,
        LocalDate effectiveFrom) {

    private static final BigDecimal CENTS = new BigDecimal("0.01");

    /**
     * Prices a quantity of units: the first {@code includedUnits} are free, the rest
     * are charged at {@code unitPrice} and rounded half up to whole cents.
     *
     * @throws IllegalArgumentException when the quantity is negative
     */
    public static BigDecimal price(RateCard card, BigDecimal quantity) {
        if (quantity == null) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        if (quantity.signum() < 0) {
            throw new IllegalArgumentException("quantity must not be negative: " + quantity);
        }
        if (quantity.signum() == 0) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        BigDecimal billable = quantity.subtract(card.includedUnits());
        if (billable.signum() <= 0) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        return billable.multiply(card.unitPrice()).setScale(2, RoundingMode.HALF_UP);
    }

    /** Loads the rate cards that are in force on the given day. */
    @Repository
    public static class RateCardRepository {

        private static final String ACTIVE_SQL = """
                SELECT meter, version, unit_price, included_units, effective_from
                FROM rate_cards
                WHERE effective_from <= ?
                ORDER BY meter, version DESC
                """;

        private static final RowMapper<RateCard> MAPPER = (rs, rowNum) -> new RateCard(
                rs.getString("meter"),
                rs.getInt("version"),
                rs.getBigDecimal("unit_price"),
                rs.getBigDecimal("included_units"),
                rs.getDate("effective_from").toLocalDate());

        private final JdbcTemplate jdbc;

        public RateCardRepository(JdbcTemplate jdbc) {
            this.jdbc = jdbc;
        }

        /**
         * Returns one card per meter: the newest version whose effective date is on or
         * before {@code day}. Meters with no card yet are left out.
         */
        public List<RateCard> active(LocalDate day) {
            List<RateCard> rows = jdbc.query(ACTIVE_SQL, MAPPER, Date.valueOf(day));
            return rows.stream()
                    .collect(java.util.stream.Collectors.toMap(
                            RateCard::meter,
                            card -> card,
                            (first, second) -> first.version() >= second.version() ? first : second,
                            java.util.LinkedHashMap::new))
                    .values()
                    .stream()
                    .toList();
        }

        /** Looks up the card for one meter on a day, or null when the meter is unpriced. */
        public RateCard forMeter(String meter, LocalDate day) {
            return active(day).stream()
                    .filter(card -> card.meter().equals(meter))
                    .findFirst()
                    .orElse(null);
        }

        /** Prices a quantity with the card in force for that meter on that day. */
        public static BigDecimal price(RateCard card, BigDecimal quantity) {
            return RateCard.price(card, quantity);
        }
    }
}
