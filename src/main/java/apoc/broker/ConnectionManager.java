package apoc.broker;

import apoc.Pools;
import org.neo4j.logging.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ConnectionManager
{
    static ExecutorService pool = Pools.DEFAULT;

    private ConnectionManager()
    {
    }

    private static Map<String,BrokerConnection> brokerConnections = new HashMap<>();

    public static BrokerConnection addRabbitMQConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        log.info( "APOC Broker: Adding RabbitMQ Connection '" + connectionName + "' with configurations " + configuration.toString() );
        return brokerConnections.put( connectionName, RabbitMqConnectionFactory.createConnection( connectionName, log, configuration ) );
    }

    public static BrokerConnection addSQSConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        log.info( "APOC Broker: Adding SQS Connection '" + connectionName + "' with configurations " + configuration.toString() );
        return brokerConnections.put( connectionName, SqsConnectionFactory.createConnection( connectionName, log, configuration ) );
    }

    public static BrokerConnection addKafkaConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        log.info( "APOC Broker: Adding Kafka Connection '" + connectionName + "' with configurations " + configuration.toString() );
        return brokerConnections.put( connectionName, KafkaConnectionFactory.createConnection( connectionName, log, configuration ) );
    }

    public static void asyncReconnect( String connectionName ) throws Exception
    {
        Future<BrokerConnection> brokerConnectionFuture = pool.submit( () -> {
            BrokerConnection brokerConnection = ConnectionFactory.reconnect( (brokerConnections.get( connectionName )) );
            brokerConnections.put( connectionName, brokerConnection );
            BrokerIntegration.BrokerHandler.setBrokerConnections( brokerConnections );
            return brokerConnection;
        } );

//        brokerConnections.put( connectionName, brokerConnectionFuture.get() );
//        BrokerIntegration.BrokerHandler.setBrokerConnections( brokerConnections );

    }

    public static void closeConnections()
    {
        brokerConnections.forEach( ( name, connection ) -> connection.stop() );
    }

    public static Map<String,BrokerConnection> getBrokerConnections()
    {
        return brokerConnections;
    }
}
