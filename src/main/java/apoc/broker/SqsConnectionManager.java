package apoc.broker;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

public class SqsConnectionManager
{

    private static Map<String,SqsConnection> sqsConnectionMap = new HashMap<>();

    public static class SqsConnection implements BrokerConnection
    {

        private Log log;
        private String connectionName;
        private Map<String,Object> configuration;
        private AmazonSQS amazonSQS;

        private static ObjectMapper objectMapper = new ObjectMapper();

        public SqsConnection( Log log, String connectionName, Map<String,Object> configuration )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;

            amazonSQS = AmazonSQSClientBuilder.standard().withCredentials( new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials( (String) configuration.get( "access_key_id" ), (String) configuration.get( "secret_key_id" ) ) ) ).build();
        }

        @Override
        public Stream<BrokerMessage> sendMessage( @Name( "message" ) Map<String,Object> message, @Name( "parameters" ) Map<String,Object> parameters )
        {
            if ( !parameters.containsKey( "queueName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'queueName' in parameters missing" );
            }
            if ( !parameters.containsKey( "region" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'region' in parameters missing" );
            }

            String queueName = (String) parameters.get( "queueName" );
            String region = (String) parameters.get( "region" );

            if ( doesQueueExistInRegion( queueName, region ) )
            {
                try
                {
                    amazonSQS.sendMessage( new SendMessageRequest().withQueueUrl( queueName ).withMessageBody( objectMapper.writeValueAsString( message ) ) );
                }
                catch ( Exception e )
                {
                    log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
                }
            }
            else
            {
                log.error(
                        "Broker Exception. Connection Name: " + connectionName + ". Error: SQS queue '" + queueName + "' does not exist in region '" + region +
                                "'." );
            }

            return Stream.of( new BrokerMessage( connectionName, message, parameters ) );
        }

        @Override
        public void stop()
        {
            try
            {
                amazonSQS.shutdown();
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Failed to stop(). Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        private Boolean doesQueueExistInRegion( final String queueName, final String region )
        {
            for ( String queueUrl : amazonSQS.listQueues().getQueueUrls() )
            {
                if ( queueUrl.matches( ".*sqs[.]" + StringUtils.lowerCase( region ) + "[.]amazonaws[.]com.*" ) && queueUrl.endsWith( "/" + queueName ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

    public static SqsConnection addConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        SqsConnection sqsConnection = new SqsConnection( log, connectionName, configuration );
        sqsConnectionMap.put( connectionName, sqsConnection );
        return sqsConnection;
    }

    public static SqsConnection getConnection( String connectionName )
    {
        return sqsConnectionMap.get( connectionName );
    }

    public static void removeConnection( String connectionName )
    {
        sqsConnectionMap.get( connectionName ).stop();
        sqsConnectionMap.put( connectionName, null );
    }

    public static Stream<BrokerMessage> sendMessage( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
            @Name( "queueName" ) String queueName, @Name( "region" ) String region ) throws IOException
    {
        if ( !sqsConnectionMap.containsKey( connectionName ) )
        {
            throw new IOException( "Broker Exception. Connection '" + connectionName + "' is not a configured SQS broker connection." );
        }
        Map<String,Object> parameters = new HashMap<>();
        parameters.put( "queueName", queueName );
        parameters.put( "region", region );
        return (sqsConnectionMap.get( connectionName )).sendMessage( message, parameters );
    }
}
