package apoc.broker.logging;

import apoc.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.File;
import java.util.Map;

public class BrokerLogger
{

    private static final ObjectMapper OBJECT_MAPPER = JsonUtil.OBJECT_MAPPER;

    @JsonAutoDetect
    public static class LogEntry
    {
        private String connectionName;
        private Map<String,Object> message;
        private Map<String,Object> configuration;

        public LogEntry()
        {
        }

        public LogEntry( String connectionName, Map<String,Object> message, Map<String,Object> configuration )
        {
            this.connectionName = connectionName;
            this.message = message;
            this.configuration = configuration;
        }

        public String getConnectionName()
        {
            return connectionName;
        }

        public void setConnectionName( String connectionName )
        {
            this.connectionName = connectionName;
        }

        public Map<String,Object> getMessage()
        {
            return message;
        }

        public void setMessage( Map<String,Object> message )
        {
            this.message = message;
        }

        public Map<String,Object> getConfiguration()
        {
            return configuration;
        }

        public void setConfiguration( Map<String,Object> configuration )
        {
            this.configuration = configuration;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }

            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            LogEntry logEntry = (LogEntry) o;

            return new EqualsBuilder().append( connectionName, logEntry.connectionName ).append( message, logEntry.message ).append( configuration,
                    logEntry.configuration ).isEquals();
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder( 17, 37 ).append( connectionName ).append( message ).append( configuration ).toHashCode();
        }

        @Override
        public String toString()
        {
            return new ToStringBuilder( this ).append( "connectionName", connectionName ).append( "message", message ).append( "configuration",
                    configuration ).toString();
        }
    }

    private static String dirPath;
    private static String logName;
    private static BrokerLogService brokerLogService;

    public static void initializeBrokerLogger( GraphDatabaseAPI api, Map<String,Object> logConfiguration )
    {
        if ( logConfiguration.containsKey( "dirPath" ) )
        {
            dirPath = (String) logConfiguration.get( "dirPath" );
            logName = logConfiguration.containsKey( "logName" ) ? (String) logConfiguration.get( "logName" ) : BrokerLogService.DEFAULT_LOG_NAME;

            try
            {
                brokerLogService =
                        BrokerLogService.inLogsDirectory( api.getDependencyResolver().resolveDependency( FileSystemAbstraction.class ), new File( dirPath ),
                                logName );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "APOC Broker Exception. Logger file not found." );
            }
        }
    }

    public static void info( LogEntry logEntry ) throws Exception
    {
        info( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    public static void warn( LogEntry logEntry ) throws Exception
    {
        warn( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    public static void debug( LogEntry logEntry ) throws Exception
    {
        debug( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    public static void error( LogEntry logEntry ) throws Exception
    {
        error( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    private static void info( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).info( msg );
    }

    private static void warn( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).warn( msg );
    }

    private static void debug( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).debug( msg );
    }

    private static void error( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).error( msg );
    }

    public static String getDirPath()
    {
        return dirPath;
    }

    public static String getLogName()
    {
        return logName;
    }
}
