package apoc.broker;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.procedure.Name;

public interface BrokerConnection
{

    Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> configuration );

    void stop();
}
