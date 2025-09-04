package flying.duck;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Server {
    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        RootAllocator allocator = new RootAllocator();
        Properties readOnlyProperty = new Properties();
        readOnlyProperty.setProperty("duckdb.read_only", "true");
        try (FlightServer server = FlightServer.builder().location(Location.forGrpcInsecure("localhost", 8815))
                .producer(new DuckFlightSqlProducer("localhost", 8815,
                        allocator,
                        (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + args[0], readOnlyProperty),
                        256))
                .allocator(allocator)
                .build()) {

            server.start();
            System.out.printf("Server started. Listening on port %d%n", server.getPort());
            server.awaitTermination();
        }
    }
}
