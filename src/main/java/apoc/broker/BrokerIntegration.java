package apoc.broker;

import apoc.ApocConfiguration;
import apoc.Pools;
import apoc.broker.logging.BrokerLogger;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static apoc.broker.ConnectionManager.getConnection;
import static apoc.broker.ConnectionManager.doesExist;

/**
 *
 * Integrates the various broker pieces together.
 *
 * @author alexanderiudice
 * @since 2018.09
 */
public class BrokerIntegration
{

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.send(connectionName, message, configuration) - Send a message to the broker associated with the connectionName namespace. Takes in parameter which are dependent on the broker being used." )
    public Stream<BrokerMessage> send( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
            @Name( "configuration" ) Map<String,Object> configuration ) throws Exception
    {

        return BrokerHandler.sendMessageToBrokerConnection( connectionName, message, configuration );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.receive(connectionName, configuration) - Receive a message from the broker associated with the connectionName namespace. Takes in a configuration map which is dependent on the broker being used." )
    public Stream<BrokerResult> receive( @Name( "connectionName" ) String connectionName, @Name( "configuration" ) Map<String,Object> configuration )
            throws IOException
    {

        return BrokerHandler.receiveMessageFromBrokerConnection( connectionName, configuration );
    }

    public enum BrokerType
    {
        RABBITMQ,
        SQS,
        KAFKA
    }

    public static class BrokerHandler
    {
        private static Log neo4jLog;

        public BrokerHandler( Log log )
        {
            neo4jLog = log;

            if(BrokerLogger.getNumLogEntries() > 0L)
            {
                retryMessagesAsynch();
            }
        }

        public static Stream<BrokerMessage> sendMessageToBrokerConnection( String connection, Map<String,Object> message, Map<String,Object> configuration )
                throws Exception
        {
            if ( !doesExist( connection ) )
            {
                throw new IOException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            BrokerConnection brokerConnection = getConnection( connection );
            try {
                brokerConnection.checkConnectionHealth();
                Stream<BrokerMessage> brokerMessageStream = brokerConnection.send( message, configuration );

                Pools.DEFAULT.execute( (Runnable) () -> retryMessagesForConnectionAsynch( connection ) );


                return brokerMessageStream;
            }
            catch ( Exception e )
            {
                BrokerLogger.error( new BrokerLogger.LogLine.LogEntry( connection, message, configuration ) );
                brokerConnection.setConnected( false );


                if (!brokerConnection.isReconnecting())
                {
                    reconnectAndResendAsync( connection );
                }
                if (BrokerLogger.IsAtThreshold())
                {
                    Pools.DEFAULT.execute( (Runnable) () -> retryMessagesAsynch() );
                }
            }
            throw new RuntimeException( "Unable to send message to connection '" + connection + "'. Logged in '" + BrokerLogger.getLogName() + "'." );
        }

        public static Stream<BrokerResult> receiveMessageFromBrokerConnection( String connection, Map<String,Object> configuration ) throws IOException
        {
            if ( !doesExist( connection ) )
            {
                throw new IOException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            return getConnection( connection ).receive( configuration );
        }

        public static void retryMessagesForConnectionAsynch( String connectionName )
        {
            try
            {
                if (getConnection( connectionName ).isConnected())
                {
                    neo4jLog.info( "APOC Broker: Resending messages for '" + connectionName + "'." );
                    List<BrokerLogger.LogLine> linesToRemove = new ArrayList<>();
                    BrokerLogger.streamLogLines( connectionName ).parallel().forEach( ( ll ) -> {
                        BrokerLogger.LogLine.LogEntry logEntry = ll.getLogEntry();
                        Boolean resent = resendBrokerMessage( logEntry.getConnectionName(), logEntry.getMessage(), logEntry.getConfiguration() );
                        if ( resent )
                        {
                            //Send successfull. Now delete.
                            linesToRemove.add( ll );
                        }
                    } );
                    if ( !linesToRemove.isEmpty() )
                    {
                        BrokerLogger.removeLogLineBatch( linesToRemove );
                    }
                }
            }
            catch ( Exception e )
            {

            }

        }

        public static void retryMessagesAsynch( )
        {
            try
            {
                neo4jLog.info( "APOC Broker: Resending messages for all healthy connections." );
                List<BrokerLogger.LogLine> linesToRemove = new ArrayList<>(  );
                BrokerLogger.streamLogLines( ).parallel().forEach( (ll) ->
                {
                    BrokerLogger.LogLine.LogEntry logEntry = ll.getLogEntry();
                    if(getConnection( logEntry.getConnectionName()).isConnected())
                    {
                        Boolean b = resendBrokerMessage( logEntry.getConnectionName(), logEntry.getMessage(), logEntry.getConfiguration() );
                        if ( b )
                        {
                            //Send successfull. Now delete.
                            linesToRemove.add( ll );
                        }
                        if ( linesToRemove.size() > 10 )
                        {
                            BrokerLogger.removeLogLineBatch( linesToRemove );
                            linesToRemove.clear();
                        }
                    }

                });
                if (!linesToRemove.isEmpty())
                {
                    BrokerLogger.removeLogLineBatch( linesToRemove );
                }
            }
            catch ( Exception e )
            {

            }

        }

        public static Boolean resendBrokerMessage( String connection, Map<String,Object> message, Map<String,Object> configuration )
        {
            if ( !doesExist( connection ))
            {
                throw new RuntimeException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            try
            {
                getConnection( connection ).send( message, configuration );
            }
            catch (Exception e )
            {
                return false;
            }
            return true;
        }

        public static void reconnectAndResendAsync(String connectionName)
        {
            Pools.DEFAULT.execute( () -> {
                BrokerConnection reconnect = ConnectionFactory.reconnect( getConnection( connectionName ) );
                neo4jLog.info( "APOC Broker: Connection '" + connectionName + "' reconnected." );
                ConnectionManager.updateConnection( connectionName, reconnect);
                retryMessagesForConnectionAsynch( connectionName );
            } );
        }
    }

    public static class BrokerLifeCycle
    {
        private final Log log;
        private final GraphDatabaseAPI db;

        private static final String LOGS_CONFIG = "logs";

        public BrokerLifeCycle(  GraphDatabaseAPI db, Log log)
        {
            this.log = log;
            this.db = db;
        }

        private static String getBrokerConfiguration( String connectionName, String key )
        {
            Map<String,Object> value = ApocConfiguration.get( "broker." + connectionName );

            if ( value == null )
            {
                throw new RuntimeException( "No apoc.broker." + connectionName + " specified" );
            }
            return (String) value.get( key );
        }

        public void start()
        {
            Map<String,Object> value = ApocConfiguration.get( "broker" );

            Set<String> connectionList = new HashSet<>();

            value.forEach( ( configurationString, object ) -> {
                String connectionName = configurationString.split( "\\." )[0];

                if ( connectionName.equals( LOGS_CONFIG ) )
                {
                    BrokerLogger.initializeBrokerLogger( db, ApocConfiguration.get( "broker." + LOGS_CONFIG ) );
                }
                else
                {
                    connectionList.add( connectionName );
                }
            } );

            for ( String connectionName : connectionList )
            {

                BrokerType brokerType = BrokerType.valueOf( StringUtils.upperCase( getBrokerConfiguration( connectionName, "type" ) ) );
                Boolean enabled = Boolean.valueOf( getBrokerConfiguration( connectionName, "enabled" ) );

                if ( enabled )
                {
                    switch ( brokerType )
                    {
                    case RABBITMQ:
                        ConnectionManager.addRabbitMQConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    case SQS:
                        ConnectionManager.addSQSConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    case KAFKA:
                        ConnectionManager.addKafkaConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    default:
                        break;
                    }
                }
            }

            new BrokerHandler( log );
        }

        public void stop()
        {
            ConnectionManager.closeConnections();
        }
    }
}
