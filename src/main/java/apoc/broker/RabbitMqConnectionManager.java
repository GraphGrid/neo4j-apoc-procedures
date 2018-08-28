package apoc.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

public class RabbitMqConnectionManager
{

    private static Map<String,RabbitMqConnection> rabbitMqConnectionMap = new HashMap<>();

    public static class RabbitMqConnection implements BrokerConnection
    {
        private Log log;
        private String connectionName;
        private Map<String,Object> configuration;
        private ConnectionFactory connectionFactory = new ConnectionFactory();
        private Connection connection;
        private Channel channel;
        private static ObjectMapper objectMapper = new ObjectMapper();

        public RabbitMqConnection( Log log, String connectionName, Map<String,Object> configuration )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;
            try
            {
                this.connectionFactory.setUsername( (String) configuration.get( "username" ) );
                this.connectionFactory.setPassword( (String) configuration.get( "password" ) );
                this.connectionFactory.setVirtualHost( (String) configuration.get( "vhost" ) );
                this.connectionFactory.setHost( (String) configuration.get( "host" ) );
                this.connectionFactory.setPort( Integer.parseInt( (String) configuration.get( "port" ) ) );

                this.connection = this.connectionFactory.newConnection();

                this.channel = this.connection.createChannel();
            }
            catch ( Exception e )
            {
                this.log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        @Override
        public Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> configuration )
        {
            if ( !configuration.containsKey( "exchangeName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'exchangeName' in parameters missing" );
            }
            if ( !configuration.containsKey( "queueName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'queueName' in parameters missing" );
            }
            if ( !configuration.containsKey( "routingKey" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'routingKey' in parameters missing" );
            }

            String exchangeName = (String) configuration.get( "exchangeName" );
            String queueName = (String) configuration.get( "queueName" );
            String routingKey = (String) configuration.get( "routingKey" );
            try
            {
                // TODO: Have the 'type' of exchange be configurable. Have 'props' when sending message be configurable. Add way to retry the message?

                // Ensure the exchange and queue are declared.
                channel.exchangeDeclare( exchangeName, "topic", true );
                channel.queueDeclarePassive( queueName );

                // Ensure the exchange and queue are bound by the routing key.
                channel.queueBind( queueName, exchangeName, routingKey );

                // Get the message bytes and send the message bytes.
                channel.basicPublish( exchangeName, routingKey, null, objectMapper.writeValueAsBytes( message ) );
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
            }

            return Stream.of( new BrokerMessage( connectionName, message, configuration ) );
        }

        @Override
        public void stop()
        {
            try
            {
                channel.close();
                connection.close();
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Failed to stop(). Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }
    }

    public static RabbitMqConnection addConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        RabbitMqConnection rabbitMqConnection = new RabbitMqConnection( log, connectionName, configuration );
        rabbitMqConnectionMap.put( connectionName, rabbitMqConnection );
        return rabbitMqConnection;
    }

    public static RabbitMqConnection getConnection( String connectionName )
    {
        return rabbitMqConnectionMap.get( connectionName );
    }

    public static void removeConnection( String connectionName )
    {
        rabbitMqConnectionMap.get( connectionName ).stop();
        rabbitMqConnectionMap.put( connectionName, null );
    }

    public static Stream<BrokerMessage> send( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
            @Name( "queueName" ) String queueName, @Name( "exchangeName" ) String exchangeName, @Name( "routingKey" ) String routingKey ) throws IOException
    {
        if ( !rabbitMqConnectionMap.containsKey( connectionName ) )
        {
            throw new IOException( "Broker Exception. Connection '" + connectionName + "' is not a configured RabbitMq broker connection." );
        }
        Map<String,Object> configuration = new HashMap<>();
        configuration.put( "queueName", queueName );
        configuration.put( "exchangeName", exchangeName );
        configuration.put( "routingKey", routingKey );
        return (rabbitMqConnectionMap.get( connectionName )).send( message, configuration );
    }
}
