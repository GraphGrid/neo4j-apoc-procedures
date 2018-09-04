package apoc.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
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
        private KafkaConsumer<String,byte[]> kafkaConsumer;

        private static ObjectMapper objectMapper = new ObjectMapper();
        private static final Integer pollTries = 5;

        public KafkaConnection( Log log, String connectionName, Map<String,Object> configuration )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;

            Properties producerProperties = new Properties();
            producerProperties.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) configuration.get( "bootstrapServersConfig" ) );
            producerProperties.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer" );
            producerProperties.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer" );

            kafkaProducer = new KafkaProducer<String,byte[]>( producerProperties );

            Properties consumerProperties = new Properties();
            consumerProperties.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) configuration.get( "bootstrapServersConfig" ) );
            consumerProperties.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer" );
            consumerProperties.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer" );
            consumerProperties.setProperty( ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1" );
            consumerProperties.setProperty( ConsumerConfig.GROUP_ID_CONFIG, (String) configuration.get( "groupIdConfig" ) );

            kafkaConsumer = new KafkaConsumer<String,byte[]>( consumerProperties );
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
        public Stream<BrokerResponse> receive( @Name( "configuration" ) Map<String,Object> configuration ) throws IOException
        {

            // Topic is required
            if ( !configuration.containsKey( "topic" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'topic' in configuration missing" );
            }
            BrokerResponse brokerResponse = new BrokerResponse();
            brokerResponse.connectionName = connectionName;

            Integer noRecordsCount = 0;
            synchronized ( noRecordsCount )
            {
                if ( !kafkaConsumer.subscription().contains( (String) configuration.get( "topic" ) ) )
                {
                    kafkaConsumer.subscribe( Collections.singletonList( (String) configuration.get( "topic" ) ) );
                }


                while ( noRecordsCount < pollTries )
                {
                    if (brokerResponse.response != null)
                    {
                        break;
                    }
                    final ConsumerRecords<String,byte[]> consumerRecords = kafkaConsumer.poll( Duration.ofSeconds( 1 ) );

                    if ( consumerRecords.count() == 0 )
                    {
                        noRecordsCount++;
                    }

                    consumerRecords.forEach( record ->
                    {
                        try
                        {
                            brokerResponse.response = objectMapper.readValue( record.value(), Map.class );
                        }
                        catch ( Exception e )
                        {
                            log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
                        }
                    } );

                    kafkaConsumer.commitAsync();
                }
            }

            return Stream.of( brokerResponse );
        }

        @Override
        public void stop()
        {
            try
            {
                kafkaProducer.close();
                kafkaConsumer.close();
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
