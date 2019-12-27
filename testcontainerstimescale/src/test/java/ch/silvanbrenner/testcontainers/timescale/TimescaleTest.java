package ch.silvanbrenner.testcontainers.timescale;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TimescaleTest {

    private final static Set<HikariDataSource> datasourcesForCleanup = new HashSet<>();

    @Test
    public void testSimple() throws SQLException {
        try (TimescaleDbContainer timescaleDbContainer = new TimescaleDbContainer())  {
            timescaleDbContainer.start();

            ResultSet resultSet = performQuery(timescaleDbContainer, "SELECT 1");
            int resultSetInt = resultSet.getInt(1);
            assertEquals("A basic SELECT query succeeds", 1, resultSetInt);
        }
    }

    @Test
    public void testCommandOverride() throws SQLException {
        try (TimescaleDbContainer timescaleDbContainer = new TimescaleDbContainer<>().withCommand("postgres -c max_connections=42")) {
            timescaleDbContainer.start();

            ResultSet resultSet = performQuery(timescaleDbContainer, "SELECT current_setting('max_connections')");
            String result = resultSet.getString(1);
            assertEquals("max_connections should be overriden", "42", result);
        }
    }

    @Test
    public void testUnsetCommand() throws SQLException {
        try (TimescaleDbContainer timescaleDbContainer = new TimescaleDbContainer<>().withCommand("postgres -c max_connections=42").withCommand()) {
            timescaleDbContainer.start();

            ResultSet resultSet = performQuery(timescaleDbContainer, "SELECT current_setting('max_connections')");
            String result = resultSet.getString(1);
            assertNotEquals("max_connections should not be overriden", "42", result);
        }
    }

    @Test
    public void testExplicitInitScript() throws SQLException {
        try (TimescaleDbContainer timescaleDbContainer = new TimescaleDbContainer<>().withInitScript("init_timescale.sql")) {
            timescaleDbContainer.start();

            ResultSet resultSet = performQuery(timescaleDbContainer, "SELECT foo FROM bar");

            String firstColumnValue = resultSet.getString(1);
            assertEquals("Value from init script should equal real value", "hello world", firstColumnValue);
        }
    }

    private ResultSet performQuery(TimescaleDbContainer container, String sql) throws SQLException {
        DataSource ds = getDataSource(container);
        Statement statement = ds.getConnection().createStatement();
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();

        resultSet.next();
        return resultSet;
    }

    DataSource getDataSource(JdbcDatabaseContainer container) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());

        final HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        datasourcesForCleanup.add(dataSource);

        return dataSource;
    }

    @AfterAll
    public static void teardown() {
        datasourcesForCleanup.forEach(HikariDataSource::close);
    }
}
