package apoc.broker.logging;

import apoc.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author alexanderiudice
 */
public class BrokerLogger
{

    private static final ObjectMapper OBJECT_MAPPER = JsonUtil.OBJECT_MAPPER;

    @JsonAutoDetect
    public static class LogLine
    {
        private String time;
        private String level;
        private String logName;
        private LogEntry logEntry;

        public LogLine()
        {
        }

        public LogLine( String time, String level, String logName, LogEntry logEntry )
        {
            this.time = time;
            this.level = level;
            this.logName = logName;
            this.logEntry = logEntry;
        }

        public LogLine( String logLine )
        {
            String[] splited = logLine.split( "\\s+", 5 );

            time = splited[0] + " " + splited[1];
            level = splited[2];
            logName = splited[3];
            try
            {
                logEntry = OBJECT_MAPPER.readValue( splited[4], LogEntry.class );
            }
            catch ( Exception e )
            {
                logEntry = new LogEntry();
            }
        }

        public String getTime()
        {
            return time;
        }

        public void setTime( String time )
        {
            this.time = time;
        }

        public String getLevel()
        {
            return level;
        }

        public void setLevel( String level )
        {
            this.level = level;
        }

        public String getLogName()
        {
            return logName;
        }

        public void setLogName( String logName )
        {
            this.logName = logName;
        }

        public LogEntry getLogEntry()
        {
            return logEntry;
        }

        public void setLogEntry( LogEntry logEntry )
        {
            this.logEntry = logEntry;
        }

        public String getLogString()
        {
            String result = "";

            result += time + " " + level + " " + logName + " ";
            try
            {
                result += OBJECT_MAPPER.writeValueAsString( logEntry );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Unable to write LogEntry as String" );
            }
            return result;
        }

        @JsonAutoDetect
        public static class LogEntry
        {
            private String connectionName;
            private Map<String,Object> message;
            private Map<String,Object> configuration;

            public LogEntry()
            {
                connectionName = "";
                message = new HashMap<>();
                configuration = new HashMap<>();
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

            LogLine logLine = (LogLine) o;

            return new EqualsBuilder().append( time, logLine.time ).append( level, logLine.level ).append( logName, logLine.logName ).append( logEntry,
                    logLine.logEntry ).isEquals();
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder( 17, 37 ).append( time ).append( level ).append( logName ).append( logEntry ).toHashCode();
        }
    }

    private static String dirPath;
    private static String logName;
    private static BrokerLogService brokerLogService;

    private static File logFile;

    private static AtomicLong numLogEntries = new AtomicLong( 0L );
    private static final Long retryThreshold = 20L;

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

                logFile = new File( dirPath + logName );

                // Get the number of log file entries and set numLogEntries.
                setNumLogEntries( calculateNumberOfLogEntries() );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "APOC Broker Exception. Logger failed to initialize." );
            }
        }
    }

    public static Stream<List<LogLine.LogEntry>> batchConnectionMessages( String connectionName, int batchSize ) throws Exception
    {

        Stream<String> stream = Files.lines( Paths.get( logFile.getPath() ) );

        final Stream<LogLine.LogEntry> logEntryStream =
                stream.map( LogLine::new ).filter( logLine -> logLine.logEntry.getConnectionName().equals( connectionName ) ).map(
                        logLine -> logLine.getLogEntry() );

        return Lists.partition( logEntryStream.collect( Collectors.toList() ), batchSize ).stream();
    }

    public static Stream<LogLine> streamLogLines() throws Exception
    {
        return Files.lines( Paths.get( logFile.getPath() ) ).map( LogLine::new );
    }

    public static Stream<LogLine> streamLogLines( String connectionName ) throws Exception
    {
        return Files.lines( Paths.get( logFile.getPath() ) ).map( LogLine::new ).filter(
                logLine -> logLine.logEntry.getConnectionName().equals( connectionName ) );
    }

    public static Long calculateNumberOfLogEntries() throws Exception
    {
        return Files.lines( Paths.get( logFile.getPath() ) ).count();
    }

    public static void removeLogLineBatch( List<LogLine> logLines )
    {
        synchronized ( logFile )
        {
            try
            {
                // Could this file cause race conditions?
                File tmpFile = File.createTempFile( RandomStringUtils.randomAlphabetic( 5 ), ".log", logFile.getParentFile() );

                DataOutputStream dataOutputStream = new DataOutputStream( new FileOutputStream( tmpFile ) );

                streamLogLines().filter( ( logLine ) -> !logLines.contains( logLine ) ).forEach( logLine -> {
                    try
                    {
                        dataOutputStream.write( logLine.getLogString().getBytes() );
                        dataOutputStream.writeChars( System.getProperty( "line.separator" ) );
                    }
                    catch ( Exception e )
                    {
                        throw new RuntimeException( "Failure to remove LogLines. Failure to write LogLine to temp file." );
                    }
                } );
                org.apache.commons.io.FileUtils.copyFile( tmpFile, logFile );
                org.apache.commons.io.FileUtils.deleteQuietly( tmpFile );

                numLogEntries.getAndAdd( (-1) * logLines.size() );
            }
            catch ( Exception e )
            {
                // Make sure the tmp file got deleted.
            }
        }
    }

    public static Boolean IsAtThreshold()
    {
        return (numLogEntries.get() > retryThreshold);
    }

    public static void info( LogLine.LogEntry logEntry ) throws Exception
    {
        info( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    public static void warn( LogLine.LogEntry logEntry ) throws Exception
    {
        warn( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    public static void debug( LogLine.LogEntry logEntry ) throws Exception
    {
        debug( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    public static void error( LogLine.LogEntry logEntry ) throws Exception
    {
        error( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    private static void info( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).info( msg );
        incrementNumLogEntries();
    }

    private static void warn( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).warn( msg );
        incrementNumLogEntries();
    }

    private static void debug( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).debug( msg );
        incrementNumLogEntries();
    }

    private static void error( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).error( msg );
        incrementNumLogEntries();
    }

    public static String getDirPath()
    {
        return dirPath;
    }

    public static String getLogName()
    {
        return logName;
    }

    public static Long incrementNumLogEntries()
    {
        return numLogEntries.getAndIncrement();
    }

    public static Long decrementNumLogEntries()
    {
        return numLogEntries.getAndDecrement();
    }

    public static Long setNumLogEntries( Long numLogEntries )
    {
        return BrokerLogger.numLogEntries.getAndSet( numLogEntries );
    }

    public static Long getNumLogEntries()
    {
        return numLogEntries.get();
    }
}
