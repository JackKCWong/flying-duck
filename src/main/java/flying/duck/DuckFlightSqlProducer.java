package flying.duck;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.jdbcToArrowSchema;

public class DuckFlightSqlProducer extends BasicFlightSqlProducer {
    private static final Calendar DEFAULT_CALENDAR = JdbcToArrowUtils.getUtcCalendar();
    private static final IpcOption IPC_OPTION = new IpcOption(false, MetadataVersion.V5);

    private final String host;
    private final int port;
    private final RootAllocator allocator;
    private final DuckDBConnection backendConn;
    private final ConcurrentHashMap<ByteString, ArrowReader> resultSetCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ByteString, PreparedStatement> preparedStatementCache = new ConcurrentHashMap<>();
    private final Location location;
    private final int batchSize;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    public DuckFlightSqlProducer(String host, int port, RootAllocator allocator, DuckDBConnection backendConn, int batchSize) {
        this.host = host;
        this.port = port;
        this.allocator = allocator;
        this.backendConn = backendConn;
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
            this.resultSetCache.put(handle, reader);

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
        ArrowReader arrowReader = this.resultSetCache.get(handle);
        if (arrowReader == null) {
            throw CallStatus.NOT_FOUND.withDescription("resultSet not found").toRuntimeException();
        }

        try {
            sendArrowStream(arrowReader, listener);
        } finally {
            this.resultSetCache.remove(handle);
        }
    }

    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        executorService.submit(() -> {
            try {
                final ByteString preparedStatementHandle =
                        copyFrom(request.getQuery().getBytes(StandardCharsets.UTF_8));
                final PreparedStatement preparedStatement = this.backendConn.prepareStatement(request.getQuery());
                this.preparedStatementCache.put(preparedStatementHandle, preparedStatement);

                final ResultSetMetaData metaData = preparedStatement.getMetaData();
                final FlightSql.ActionCreatePreparedStatementResult result =
                        FlightSql.ActionCreatePreparedStatementResult.newBuilder()
//                            .setDatasetSchema(
//                                    copyFrom(serializeMetadata(jdbcToArrowSchema(metaData, DEFAULT_CALENDAR), IPC_OPTION)))
//                            .setParameterSchema(
//                                    copyFrom(serializeMetadata(jdbcToArrowSchema(preparedStatement.getParameterMetaData(), DEFAULT_CALENDAR), IPC_OPTION)))
                                .setPreparedStatementHandle(preparedStatementHandle)
                                .build();
                listener.onNext(new Result(pack(result).toByteArray()));
                listener.onCompleted();
            } catch (SQLException e) {
                listener.onError(e);
                throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
            }
        });
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        ByteString preparedStatementHandle = request.getPreparedStatementHandle();
        try (PreparedStatement ignored = this.preparedStatementCache.remove(preparedStatementHandle)) {
            listener.onCompleted();
        } catch (SQLException e) {
            listener.onError(e);
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        }
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        ByteString preparedStatementHandle = command.getPreparedStatementHandle();
        PreparedStatement preparedStatement = this.preparedStatementCache.get(preparedStatementHandle);
        if (preparedStatement == null) {
            throw CallStatus.NOT_FOUND.withDescription("preparedStatement not found").toRuntimeException();
        }

        try {
            return FlightInfo
                    .builder(jdbcToArrowSchema(preparedStatement.getMetaData(),
                                    DEFAULT_CALENDAR), descriptor,
                            List.of(new FlightEndpoint(new Ticket(Any.pack(command).toByteArray())))
                    )
                    .build();
        } catch (SQLException e) {
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        }
    }

    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context, ServerStreamListener listener) {
        ByteString preparedStatementHandle = command.getPreparedStatementHandle();
        PreparedStatement preparedStatement = this.preparedStatementCache.get(preparedStatementHandle);
        if (preparedStatement == null) {
            throw CallStatus.NOT_FOUND.withDescription("preparedStatement not found").toRuntimeException();
        }

        executorService.submit(() -> {
            try {
                DuckDBResultSet resultSet = (DuckDBResultSet) preparedStatement.executeQuery();
                ArrowReader arrowReader = (ArrowReader) resultSet.arrowExportStream(this.allocator, this.batchSize);
                sendArrowStream(arrowReader, listener);
            } catch (SQLException e) {
                throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
            }
        });
    }

    private void sendArrowStream(ArrowReader arrowReader, ServerStreamListener listener) {
        try (arrowReader) {
            while (arrowReader.loadNextBatch()) {
                while (!listener.isReady()) Thread.sleep(10);
                VectorSchemaRoot data = arrowReader.getVectorSchemaRoot();
                listener.start(data);
                listener.putNext();
            }
            listener.completed();
        } catch (IOException | InterruptedException e) {
            listener.error(e);
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        }
    }

    private <T extends Message> List<FlightEndpoint> getFlightEndpoints(T ticket) {
        return List.of(new FlightEndpoint(new Ticket(Any.pack(ticket).toByteArray()), location));
    }

}
