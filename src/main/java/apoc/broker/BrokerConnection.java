package apoc.broker;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.procedure.Name;

public interface BrokerConnection
{

    Stream<BrokerMessage> sendMessage( @Name( "message" ) Map<String,Object> message, @Name( "parameters" ) Map<String,Object> parameters );

    void stop();
}
