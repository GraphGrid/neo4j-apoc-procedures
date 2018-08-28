package apoc.broker;

import apoc.ApocConfiguration;
import apoc.Description;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class BrokerIntegration
{

    @Procedure( mode = Mode.READ )
    @Description( "Send a message to the broker associated with the connectionName namespace. Takes in parameter which are dependent on the broker being used." )
    public Stream<BrokerMessage> sendBrokerMessage( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
            @Name( "parameters" ) Map<String,Object> parameters ) throws IOException
    {

        return BrokerHandler.sendMessage( connectionName, message, parameters );
    }

//    @Procedure( mode = Mode.READ )
//    @Description( "Send a message to the RabbitMq broker associated with the connectionName namespace." )
//    public Stream<BrokerMessage> sendRabbitMqMessage( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
//            @Name( "queueName" ) String queueName, @Name( "exchangeName" ) String exchangeName, @Name( "routingKey" ) String routingKey ) throws IOException
//    {
//
//        return RabbitMqConnectionManager.sendMessage( connectionName, message, queueName, exchangeName, routingKey );
//    }
//
//    @Procedure( mode = Mode.READ )
//    @Description( "Send a message to the Sqs broker associated with the connectionName namespace." )
//    public Stream<BrokerMessage> sendSqsMessage( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
//            @Name( "queueName" ) String queueName, @Name( "region" ) String region ) throws IOException
//    {
//
//        return SqsConnectionManager.sendMessage( connectionName, message, queueName, region );
//    }

    public enum BrokerType
    {
        RABBITMQ,
        SQS
    }

    public static class BrokerHandler
    {
        private final Log log;
        private static Map<String,BrokerConnection> brokerConnections;

        public BrokerHandler( Log log, Map<String,BrokerConnection> brokerConnections )
        {
            this.log = log;
            this.brokerConnections = brokerConnections;
        }

        public static Stream<BrokerMessage> sendMessage( String connection, Map<String,Object> message, Map<String,Object> configuration ) throws IOException
        {
            if ( !brokerConnections.containsKey( connection ) )
            {
                throw new IOException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            return (brokerConnections.get( connection )).sendMessage( message, configuration );
        }
    }

    public static class BrokerLifeCycle
    {
        private final Log log;
        private Map<String,BrokerConnection> brokerConnections;

        public BrokerLifeCycle( Log log )
        {
            this.log = log;
            brokerConnections = new HashMap<>();
        }

        public void start()
        {
            Map<String,Object> value = ApocConfiguration.get( "broker" );

            Set<String> connectionList = new HashSet<>();

            value.forEach( ( configurationString, object ) ->
            {
                String connectionName = configurationString.split( "\\." )[0];
                connectionList.add( connectionName );
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
                        brokerConnections.put( connectionName,
                                RabbitMqConnectionManager.addConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) ) );
                        break;
                    case SQS:
                        brokerConnections.put( connectionName,
                                SqsConnectionManager.addConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) ) );
                        break;
                    default:
                        break;
                    }
                }
            }

            new BrokerHandler( log, brokerConnections );
        }

        public void stop()
        {
            for ( BrokerConnection brokerConnection : brokerConnections.values() )
            {
                brokerConnection.stop();
            }
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
    }
}
