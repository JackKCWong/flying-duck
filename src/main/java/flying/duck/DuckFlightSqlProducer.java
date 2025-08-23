package flying.duck;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;

public class DuckFlightSqlProducer extends BasicFlightSqlProducer {
    private final String host;
    private final int port;
    private final RootAllocator allocator;
    private final DuckDBConnection backendConn;
    private final ConcurrentHashMap<ByteString, ArrowReader> flightReaders;
    private final Location location;
    private final int batchSize;

    public DuckFlightSqlProducer(String host, int port, RootAllocator allocator, DuckDBConnection backendConn, int batchSize) {
        this.host = host;
        this.port = port;
        this.allocator = allocator;
        this.backendConn = backendConn;
        this.flightReaders = new ConcurrentHashMap<>();
        this.location = Location.forGrpcInsecure(this.host, this.port);
        this.batchSize = batchSize;
    }

    @Override
    protected <T extends Message> List<FlightEndpoint> determineEndpoints(T request, FlightDescriptor flightDescriptor, Schema schema) {
        return getFlightEndpoints(request);
    }

    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        String sql = command.getQuery();
        try {
            ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
            Statement stmt = backendConn.createStatement();
            DuckDBResultSet rs = (DuckDBResultSet) stmt.executeQuery(sql);
            ArrowReader reader = (ArrowReader) rs.arrowExportStream(this.allocator, batchSize);
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            this.flightReaders.put(handle, reader);

            FlightSql.TicketStatementQuery ticket =
                    FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle).build();

            return FlightInfo.builder(schema, descriptor, getFlightEndpoints(ticket)).build();
        } catch (SQLException | IOException e) {
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        }
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket, CallContext context, ServerStreamListener listener) {
        ByteString handle = ticket.getStatementHandle();
        ArrowReader arrowReader = this.flightReaders.get(handle);
        try {
            while (arrowReader.loadNextBatch()) {
                listener.start(arrowReader.getVectorSchemaRoot());
                listener.putNext();
            }
            listener.completed();
        } catch (IOException e) {
            listener.error(e);
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        }
    }

    private <T extends Message> List<FlightEndpoint> getFlightEndpoints(T ticket) {
        return List.of(new FlightEndpoint(new Ticket(Any.pack(ticket).toByteArray()), location));
    }

}
