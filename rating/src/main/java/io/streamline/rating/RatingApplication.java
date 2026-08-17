package io.streamline.rating;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class RatingApplication {

    public static void main(String[] args) {
        SpringApplication.run(RatingApplication.class, args);
    }
}

@RestController
@RequestMapping("/api")
class RatingController {

    private final JdbcTemplate jdbc;
    private final RateCardRepository rateCards;
    private final LedgerWriter ledgerWriter;

    RatingController(JdbcTemplate jdbc, RateCardRepository rateCards, LedgerWriter ledgerWriter) {
        this.jdbc = jdbc;
        this.rateCards = rateCards;
        this.ledgerWriter = ledgerWriter;
    }

    /**
     * Rates every usage bucket of the given day and writes the ledger lines.
     * Running it twice for the same day is safe: the writer upserts on
     * (account, meter, hour) and keeps the rate version that priced the line.
     */
    @PostMapping("/rate")
    ResponseEntity<Map<String, Object>> rate(@RequestParam("day") String day) {
        LocalDate date;
        try {
            date = LocalDate.parse(day);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("error", "day must be YYYY-MM-DD"));
        }

        List<RateCard> cards = rateCards.active(date);
        if (cards.isEmpty()) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "no active rate card for " + date));
        }

        Instant from = date.atStartOfDay(ZoneOffset.UTC).toInstant();
        Instant to = date.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();

        List<LedgerLine> lines = jdbc.query(
                "SELECT account_id, meter, hour, quantity FROM usage_buckets "
                        + "WHERE hour >= ? AND hour < ? ORDER BY account_id, meter, hour",
                (rs, i) -> new LedgerLine(
                        rs.getString("account_id"),
                        rs.getString("meter"),
                        rs.getTimestamp("hour").toInstant(),
                        rs.getBigDecimal("quantity"),
                        BigDecimal.ZERO,
                        0),
                java.sql.Timestamp.from(from),
                java.sql.Timestamp.from(to));

        List<LedgerLine> priced = lines.stream()
                .map(line -> {
                    RateCard card = cards.stream()
                            .filter(c -> c.meter().equals(line.meter()))
                            .findFirst()
                            .orElse(null);
                    if (card == null) {
                        return null;
                    }
                    BigDecimal amount = RateCardRepository.price(card, line.quantity());
                    return new LedgerLine(line.accountId(), line.meter(), line.hour(),
                            line.quantity(), amount, card.version());
                })
                .filter(java.util.Objects::nonNull)
                .toList();

        int written = ledgerWriter.writeAll(priced);
        BigDecimal total = priced.stream()
                .map(LedgerLine::amount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        return ResponseEntity.ok(Map.of(
                "day", date.toString(),
                "buckets", lines.size(),
                "linesWritten", written,
                "unpricedMeters", lines.size() - priced.size(),
                "totalAmount", total.setScale(2, java.math.RoundingMode.HALF_UP)));
    }

    @GetMapping("/ledger")
    ResponseEntity<List<Map<String, Object>>> ledger(@RequestParam("day") String day) {
        LocalDate date;
        try {
            date = LocalDate.parse(day);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().build();
        }
        Instant from = date.atStartOfDay(ZoneOffset.UTC).toInstant();
        Instant to = date.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT account_id, meter, hour, quantity, amount, rate_version "
                        + "FROM ledger_lines WHERE hour >= ? AND hour < ? "
                        + "ORDER BY account_id, meter, hour",
                java.sql.Timestamp.from(from),
                java.sql.Timestamp.from(to));
        return ResponseEntity.ok(rows);
    }

    @GetMapping("/rate-cards")
    List<RateCard> rateCards() {
        return rateCards.active(LocalDate.now(ZoneOffset.UTC));
    }
}
