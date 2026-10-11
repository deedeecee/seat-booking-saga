package com.debankar.saga.booking.consistency;

import com.debankar.saga.common.consistency.ConsistencyChecker;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Gives {@link ConsistencyChecker} its two extra database connections —
 * booking-service's own primary {@link JdbcTemplate} already covers
 * {@code booking_db}. These two are read-only in practice (the checker
 * never writes), kept deliberately separate from the application's own
 * datasource, and sized small: the consistency check runs occasionally,
 * one query at a time per database, not under real load.
 *
 * <p>Both are declared {@code @Bean(defaultCandidate = false)}: Spring
 * Boot's own {@code JdbcTemplateAutoConfiguration} only runs when there's
 * exactly one {@code DataSource}-shaped bean in the context, and a
 * {@code HikariDataSource} is one, so without this, adding these two
 * would silently stop the autoconfigured {@code JdbcTemplate} every other
 * bean in {@code BookingConfiguration} depends on from ever being
 * created. {@code defaultCandidate = false} excludes a bean only from
 * that kind of untargeted "give me the one DataSource" lookup.
 *
 * <p>The {@code consistencyChecker} method below explicitly {@code
 * @Qualifier}s both datasources, rather than leaving Spring to
 * disambiguate two same-typed parameters by matching their names against
 * bean names — that fallback depends on the compiled class file actually
 * retaining real parameter names, which isn't guaranteed, and silently
 * swapping which pool goes where is exactly the kind of failure that
 * looks like a connection problem but isn't one.
 *
 * <p>{@code initializationFailTimeout(-1)} matters separately: by default
 * HikariCP validates a connection the moment a pool is built, and in the
 * test context — a single Testcontainers Postgres with only {@code
 * booking_db} on it — {@code inventory_db} and {@code payment_db}
 * genuinely don't exist. Without this, bean construction itself would
 * fail or hang during every context-loading test. With it, connections
 * are validated lazily, on first real use — and since the only caller is
 * the endpoint this wires into, which nothing in the test suite calls,
 * the beans construct cleanly in test and only need to actually connect
 * when this is genuinely running for real.
 */
@Configuration(proxyBeanMethods = false)
public class ConsistencyCheckerConfiguration {

    @Bean(defaultCandidate = false)
    public HikariDataSource inventoryCheckDataSource(
            @Value("${consistency.inventory-db-url}") String url,
            @Value("${DB_USER:saga}") String username,
            @Value("${DB_PASSWORD:saga}") String password) {
        return buildDataSource(url, username, password);
    }

    @Bean(defaultCandidate = false)
    public HikariDataSource paymentCheckDataSource(
            @Value("${consistency.payment-db-url}") String url,
            @Value("${DB_USER:saga}") String username,
            @Value("${DB_PASSWORD:saga}") String password) {
        return buildDataSource(url, username, password);
    }

    @Bean
    public ConsistencyChecker consistencyChecker(JdbcTemplate jdbcTemplate,
                                                 @Qualifier("inventoryCheckDataSource") HikariDataSource inventoryDataSource,
                                                 @Qualifier("paymentCheckDataSource") HikariDataSource paymentDataSource) {
        return new ConsistencyChecker(jdbcTemplate,
                new JdbcTemplate(inventoryDataSource), new JdbcTemplate(paymentDataSource));
    }

    private HikariDataSource buildDataSource(String url, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(2);
        config.setInitializationFailTimeout(-1);
        return new HikariDataSource(config);
    }
}