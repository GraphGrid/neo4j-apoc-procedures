package apoc.broker;

import java.util.Map;

public class BrokerMessage
{
    public String connectionName;
    public Map<String,Object> message;
    public Map<String,Object> parameters;

    public BrokerMessage()
    {
    }

    public BrokerMessage( String connectionName, Map<String,Object> message, Map<String,Object> parameters )
    {
        this.connectionName = connectionName;
        this.message = message;
        this.parameters = parameters;
    }
}
