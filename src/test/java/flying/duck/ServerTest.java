package flying.duck;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServerTest {

    private static FlightServer server;
    private Connection conn;

    @AfterAll
    static void tearDown() throws InterruptedException {
        server.close();
    }

    @BeforeEach
    void connect() throws SQLException {
        conn = DriverManager.getConnection("jdbc:arrow-flight-sql://localhost:8815?useEncryption=0");
    }

    @AfterEach
    void disconnect() throws SQLException {
        conn.close();
    }

    @Test
    public void shouldBeAbleToConnect() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("select 1");
        try (ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @BeforeAll
    static void setUp() throws IOException, SQLException {
        RootAllocator allocator = new RootAllocator();
        server = FlightServer.builder().location(Location.forGrpcInsecure("localhost", 8815))
                .producer(new DuckFlightSqlProducer("localhost", 8815,
                        allocator,
                        (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:src/test/resources/test.db"),
                        512))
                .allocator(new RootAllocator())
                .build();

        server.start();
        System.out.printf("Server started. Listening on port %d%n", server.getPort());
    }

    @Test
    public void shouldBeAbleToReadMultipleRecordBatches() throws SQLException, InterruptedException {
        PreparedStatement stmt = conn.prepareStatement("SELECT * from ice limit 1024");
        try (ResultSet rs = stmt.executeQuery()) {
            int count = 0;
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                ++count;
                for (int i = 1; i <= columnCount; i++) {
                    Object obj = rs.getObject(i);
                    System.out.printf("%s, ", obj.toString());
                }
                System.out.println();
            }
            assertEquals(1024, count);
        }
    }
}