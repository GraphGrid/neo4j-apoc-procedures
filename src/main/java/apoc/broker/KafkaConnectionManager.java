package apoc.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

public class KafkaConnectionManager
{

    private static Map<String,KafkaConnection> kafkaConnectionMap = new HashMap<>();

    public static class KafkaConnection implements BrokerConnection
    {
        private Log log;
        private String connectionName;
        private Map<String,Object> configuration;
        private KafkaProducer<String,byte[]> kafkaProducer;

        private static ObjectMapper objectMapper = new ObjectMapper();

        public KafkaConnection( Log log, String connectionName, Map<String,Object> configuration )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;

            Properties properties = new Properties();
            properties.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) configuration.get( "bootstrapServersConfig" ) );
            properties.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer" );
            properties.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer" );

            kafkaProducer = new KafkaProducer<String,byte[]>( properties );
            try
            {

            }
            catch ( Exception e )
            {
                this.log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        @Override
        public Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> parameters )
        {
            // Topic and value are required
            if ( !parameters.containsKey( "topic" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'topic' in parameters missing" );
            }

            String topic = (String) parameters.get( "topic" );

            Integer partition = -1;
            if ( parameters.containsKey( "partition" ) )
            {
                partition = (Integer) parameters.get( "partition" );
            }

            String key = "";
            if ( parameters.containsKey( "key" ) )
            {
                key = (String) parameters.get( "key" );
            }

            try
            {
                ProducerRecord<String,byte[]> producerRecord;
                if ( partition >= 0 && !key.isEmpty() )
                {
                    producerRecord = new ProducerRecord<>( topic, partition, key, objectMapper.writeValueAsBytes( message ) );
                }
                else if ( !key.isEmpty() )
                {
                    producerRecord = new ProducerRecord<>( topic, key, objectMapper.writeValueAsBytes( message ) );
                }
                else
                {
                    producerRecord = new ProducerRecord<>( topic, objectMapper.writeValueAsBytes( message ) );
                }

                kafkaProducer.send( producerRecord );
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
            }

            return Stream.of( new BrokerMessage( connectionName, message, parameters ) );
        }

        @Override
        public Stream<BrokerResponse> receive( @Name( "configuration" ) Map<String,Object> configuration )
        {

            return Stream.of( new BrokerResponse( connectionName, null) );
        }

        @Override
        public void stop()
        {
            try
            {
                kafkaProducer.close();
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Failed to stop(). Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }
    }

    public static KafkaConnection addConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        KafkaConnection kafkaConnection = new KafkaConnection( log, connectionName, configuration );
        kafkaConnectionMap.put( connectionName, kafkaConnection );
        return kafkaConnection;
    }

    public static KafkaConnection getConnection( String connectionName )
    {
        return kafkaConnectionMap.get( connectionName );
    }

    public static void removeConnection( String connectionName )
    {
        kafkaConnectionMap.get( connectionName ).stop();
        kafkaConnectionMap.put( connectionName, null );
        kafkaConnectionMap.remove( connectionName );
    }

    public static Stream<BrokerMessage> send( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
            @Name( "bootstrapServersConfig" ) String bootstrapServersConfig ) throws IOException
    {
        if ( !kafkaConnectionMap.containsKey( connectionName ) )
        {
            throw new IOException( "Broker Exception. Connection '" + connectionName + "' is not a configured Kafka broker connection." );
        }
        Map<String,Object> configuration = new HashMap<>();
        configuration.put( "bootstrapServersConfig", bootstrapServersConfig );
        return (kafkaConnectionMap.get( connectionName )).send( message, configuration );
    }
}
