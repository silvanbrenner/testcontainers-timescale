package ch.silvanbrenner.testcontainers.timescale;

import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static java.time.temporal.ChronoUnit.SECONDS;

public class TimescaleDbContainer<SELF extends TimescaleDbContainer<SELF>> extends JdbcDatabaseContainer<SELF> {

    public static final String IMAGE = "timescale/timescaledb";
    public static final String DEFAULT_TAG = "latest-pg11";

    public static final Integer TIMESCALEDB_PORT = 5432;
    private String databaseName = "test";
    private String username = "test";
    private String password = "test";

    private static final String FSYNC_OFF_OPTION = "fsync=off";

    public TimescaleDbContainer() {
        this(IMAGE + ":" + DEFAULT_TAG);
    }

    public TimescaleDbContainer(final String dockerImageName) {
        super(dockerImageName);
        this.waitStrategy = new LogMessageWaitStrategy()
                .withRegEx(".*database system is ready to accept connections.*\\s")
                .withTimes(2)
                .withStartupTimeout(Duration.of(60, SECONDS));
        this.setCommand("postgres", "-c", FSYNC_OFF_OPTION);
    }

    @NotNull
    @Override
    protected Set<Integer> getLivenessCheckPorts() {
        return new HashSet<>(getMappedPort(TIMESCALEDB_PORT));
    }

    @Override
    protected void configure() {
        addExposedPort(TIMESCALEDB_PORT);
        addEnv("POSTGRES_DB", databaseName);
        addEnv("POSTGRES_USER", username);
        addEnv("POSTGRES_PASSWORD", password);
    }

    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJdbcUrl() {
        // Disable Postgres driver use of java.util.logging to reduce noise at startup time
        return "jdbc:postgresql://" + getContainerIpAddress() + ":" + getMappedPort(TIMESCALEDB_PORT) + "/" + databaseName + "?loggerLevel=OFF";
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1";
    }

    @Override
    public SELF withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return self();
    }

    @Override
    public SELF withUsername(final String username) {
        this.username = username;
        return self();
    }

    @Override
    public SELF withPassword(final String password) {
        this.password = password;
        return self();
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }
}
