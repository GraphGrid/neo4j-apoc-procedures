package apoc.broker;

import java.util.Map;

public class BrokerResponse
{
    public String connectionName;
    public Map<String,Object> response;

    public BrokerResponse()
    {
    }

    public BrokerResponse( String connectionName, Map<String,Object> response )
    {
        this.connectionName = connectionName;
        this.response = response;
    }
}
