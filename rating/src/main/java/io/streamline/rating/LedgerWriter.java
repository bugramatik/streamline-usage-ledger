package io.streamline.rating;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes rated ledger lines into Postgres.
 *
 * <p>A line is identified by account, meter and hour, so replaying a day of
 * buckets updates the existing row instead of adding a second charge. The
 * quantity and amount are only overwritten when the incoming rate version is
 * the same or newer, which keeps an old replay from undoing a repriced day.
 */
@Component
public class LedgerWriter {

    private static final String UPSERT = """
            INSERT INTO ledger_lines
                (account_id, meter, hour, quantity, amount, rate_version, written_at)
            VALUES (?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (account_id, meter, hour) DO UPDATE
            SET quantity = EXCLUDED.quantity,
                amount = EXCLUDED.amount,
                rate_version = EXCLUDED.rate_version,
                written_at = now()
            WHERE ledger_lines.rate_version <= EXCLUDED.rate_version
            """;

    private final JdbcTemplate jdbc;

    public LedgerWriter(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Write every line in one transaction and return how many rows were
     * inserted or updated. Rows skipped by the rate version guard are not
     * counted.
     */
    @Transactional
    public int writeAll(List<LedgerLine> lines) {
        if (lines == null || lines.isEmpty()) {
            return 0;
        }
        int[][] counts = jdbc.batchUpdate(UPSERT, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
            @Override
            public void setValues(java.sql.PreparedStatement ps, int index) throws java.sql.SQLException {
                LedgerLine line = lines.get(index);
                ps.setString(1, line.accountId());
                ps.setString(2, line.meter());
                ps.setTimestamp(3, Timestamp.from(line.hour()));
                ps.setBigDecimal(4, line.quantity());
                ps.setBigDecimal(5, line.amount());
                ps.setInt(6, line.rateVersion());
            }

            @Override
            public int getBatchSize() {
                return lines.size();
            }
        });

        int written = 0;
        for (int[] batch : counts) {
            for (int count : batch) {
                if (count > 0) {
                    written += count;
                }
            }
        }
        return written;
    }

    /** One rated hour of one meter for one account. */
    public record LedgerLine(
            String accountId,
            String meter,
            Instant hour,
            BigDecimal quantity,
            BigDecimal amount,
            int rateVersion) {
    }
}
