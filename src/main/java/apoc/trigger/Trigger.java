package apoc.trigger;

import apoc.ApocConfiguration;
import apoc.Description;
import apoc.coll.SetBackedList;
import apoc.util.Util;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.Util.map;

//import org.neo4j.procedure.Mode;

/**
 * @author mh
 * @since 20.09.16
 */
public class Trigger {

    public static class TriggerInfo {
        public String name;
        public String query;
        public Map<String,Object> selector;
        public Map<String, Object> config;
        public boolean installed;
        public boolean paused;

        public TriggerInfo(String name, String query, Map<String, Object> selector, boolean installed, boolean paused) {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.installed = installed;
            this.paused = paused;
        }

        public TriggerInfo( String name, String query, Map<String,Object> selector, Map<String,Object> config, boolean installed, boolean paused )
        {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.config = config;
            this.installed = installed;
            this.paused = paused;
        }
    }

    @Context public GraphDatabaseService db;

    @UserFunction
    @Description("function to filter labelEntries by label, to be used within a trigger statement with {assignedLabels}, {removedLabels}, {assigned/removedNodeProperties}")
    public List<Node> nodesByLabel(@Name("labelEntries") Object entries, @Name("label") String labelString) {
        if (!(entries instanceof Map)) return Collections.emptyList();
        Map map = (Map) entries;
        if (map.isEmpty()) return Collections.emptyList();
        Object result = ((Map) entries).get(labelString);
        if (result instanceof List) return (List<Node>) result;
        Object anEntry = map.values().iterator().next();

        if (anEntry instanceof List) {
            List list = (List) anEntry;
            if (!list.isEmpty()) {
                if (list.get(0) instanceof Map) {
                    Set<Node> nodeSet = new HashSet<>(100);
                    Label label = labelString == null ? null : Label.label(labelString);
                    for (List<Map<String,Object>> entry : (Collection<List<Map<String,Object>>>) map.values()) {
                        for (Map<String, Object> propertyEntry : entry) {
                            Object node = propertyEntry.get("node");
                            if (node instanceof Node && (label == null || ((Node)node).hasLabel(label))) {
                                nodeSet.add((Node)node);
                            }
                        }
                    }
                    if (!nodeSet.isEmpty()) return new SetBackedList<>(nodeSet);
                } else if (list.get(0) instanceof Node) {
                    if (labelString==null) {
                        Set<Node> nodeSet = new HashSet<>(map.size()*list.size());
                        map.values().forEach((l) -> nodeSet.addAll((Collection<Node>)l));
                        return new SetBackedList<>(nodeSet);
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    @UserFunction
    @Description("function to filter propertyEntries by property-key, to be used within a trigger statement with {assignedNode/RelationshipProperties} and {removedNode/RelationshipProperties}. Returns [{old,new,key,node,relationship}]")
    public List<Map<String,Object>> propertiesByKey(@Name("propertyEntries") Map<String,List<Map<String,Object>>> propertyEntries, @Name("key") String key) {
        return propertyEntries.getOrDefault(key,Collections.emptyList());
    }

    private static String statement(Map<String,Object> map) {
        return (String)map.getOrDefault("statement",map.get("kernelTransaction"));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("add a trigger statement under a name, in the statement you can use {createdNodes}, {deletedNodes} etc., the selector is {phase:'before/after/rollback'} returns previous and new trigger information. Takes in an optional configuration.")
    public Stream<TriggerInfo> add(@Name("name") String name, @Name("statement") String statement, @Name(value = "selector"/*, defaultValue = "{}"*/)  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        Map<String, Object> removed = TriggerHandler.add(name, statement, selector, config);
        if (removed != null) {
            return Stream.of(
                    new TriggerInfo(name,statement(removed), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("config"),false, false),
                    new TriggerInfo(name,statement,selector, config,true, false));
        }
        return Stream.of(new TriggerInfo(name,statement,selector, config,true, false));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("remove previously added trigger, returns trigger information")
    public Stream<TriggerInfo> remove(@Name("name")String name) {
        Map<String, Object> removed = TriggerHandler.remove(name);
        if (removed == null) {
            Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(name,statement(removed), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("config"),false, false));
    }

    @PerformsWrites
    @Procedure
    @Description("list all installed triggers")
    public Stream<TriggerInfo> list() {
        return TriggerHandler.list().entrySet().stream()
                .map( (e) -> new TriggerInfo(e.getKey(),statement(e.getValue()),(Map<String,Object>)e.getValue().get("selector"), (Map<String, Object>) e.getValue().get("config"),true, (Boolean) e.getValue().get("paused")));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.pause(name) | it pauses the trigger")
    public Stream<TriggerInfo> pause(@Name("name")String name) {
        Map<String, Object> paused = TriggerHandler.paused(name);
        return Stream.of(new TriggerInfo(name,statement(paused), (Map<String,Object>) paused.get("selector"), (Map<String,Object>) paused.get("config"),true, true));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.resume(name) | it resumes the paused trigger")
    public Stream<TriggerInfo> resume(@Name("name")String name) {
        Map<String, Object> resume = TriggerHandler.resume(name);
        return Stream.of(new TriggerInfo(name,statement(resume), (Map<String,Object>) resume.get("selector"), (Map<String,Object>) resume.get("config"),true, false));
    }

    public static class TriggerHandler implements TransactionEventHandler {
        public static final String APOC_TRIGGER = "apoc.trigger";
        static ConcurrentHashMap<String,Map<String,Object>> triggers = new ConcurrentHashMap(map("",map()));
        private static GraphProperties properties;
        private final Log log;
        public TriggerHandler(GraphDatabaseAPI api, Log log) {
            properties = api.getDependencyResolver().resolveDependency(NodeManager.class).newGraphProperties();
//            Pools.SCHEDULED.submit(() -> updateTriggers(null,null));
            this.log = log;
        }

        public static Map<String, Object> add(String name, String statement, Map<String,Object> selector) {
            return add(name, statement, selector, Collections.emptyMap());
        }

        public static Map<String, Object> add(String name, String statement, Map<String,Object> selector, Map<String,Object> config) {
            return updateTriggers(name, map("statement", statement, "selector", selector, "config", config, "paused", false));
        }
        public synchronized static Map<String, Object> remove(String name) {
            return updateTriggers(name,null);
        }

        public static Map<String, Object> paused(String name) {
            Map<String, Object> triggerToPause = triggers.get(name);
            updateTriggers(name, map("statement", triggerToPause.get("statement"), "selector", triggerToPause.get("selector"), "config", triggerToPause.get("config"), "paused", true));
            return triggers.get(name);
        }

        public static Map<String, Object> resume(String name) {
            Map<String, Object> triggerToResume = triggers.get(name);
            updateTriggers(name, map("statement", triggerToResume.get("statement"), "selector", triggerToResume.get("selector"), "config", triggerToResume.get("config"), "paused", false));
            return triggers.get(name);
        }

        private synchronized static Map<String, Object> updateTriggers(String name, Map<String, Object> value) {
            try (Transaction tx = properties.getGraphDatabase().beginTx()) {
                triggers.clear();
                String triggerProperty = (String) properties.getProperty(APOC_TRIGGER, "{}");
                triggers.putAll(Util.fromJson(triggerProperty,Map.class));
                Map<String,Object> previous = null;
                if (name != null) {
                    previous = (value == null) ? triggers.remove(name) : triggers.put(name, value);
                    if (value != null || previous != null) {
                        properties.setProperty(APOC_TRIGGER, Util.toJson(triggers));
                    }
                }
                tx.success();
                return previous;
            }
        }

        public static Map<String,Map<String,Object>> list() {
            updateTriggers(null,null);
            return triggers;
        }

        @Override
        public Object beforeCommit(TransactionData txData) throws Exception {
            executeTriggers(txData, "before");
            return null;
        }

        private void executeTriggers(TransactionData txData, String phase) {
            if (triggers.containsKey("")) updateTriggers(null,null);
            GraphDatabaseService db = properties.getGraphDatabase();
            Map<String,String> exceptions = new LinkedHashMap<>();
            triggers.forEach((name, data) -> {
                Map<String, Object> params = txDataParams(txData, phase);
                if( data.get("paused").equals(false)) {
                    params.putAll( txDataCollector( txData, phase, (Map<String,Object>) data.get( "config" ) ) );
                    if( ( (Map<String,Object>) data.get( "config" )).get( "params" ) != null)
                    {
                        params.putAll( (Map<String,Object>) ((Map<String,Object>) data.get( "config" )).get( "params" ) );
                    }
                    try (Transaction tx = db.beginTx()) {
                        Map<String,Object> selector = (Map<String, Object>) data.get("selector");
                        if (when(selector, phase)) {
                            params.put("trigger", name);
                            Result result = db.execute((String) data.get("statement"), params);
                            Iterators.count(result);
                            result.close();
                        }
                        tx.success();
                    } catch(Exception e) {
                        log.warn("Error executing trigger "+name+" in phase "+phase,e);
                        exceptions.put(name, e.getMessage());
                    }
                }
            });
            if (!exceptions.isEmpty()) {
                throw new RuntimeException("Error executing triggers "+exceptions.toString());
            }
        }

        private boolean when(Map<String, Object> selector, String phase) {
            if (selector == null) return (phase.equals("before"));
            return selector.getOrDefault("phase", "before").equals(phase);
        }
        
        private Map<String,Object> txDataCollector( TransactionData txData, String phase, Map<String,Object> config)
        {
            Map<String,Object> txDataMap = new HashMap<>();
            GraphDatabaseService db = properties.getGraphDatabase();

            String uidKey = (String) config.getOrDefault( "uidKey", "" );
            Boolean nodePropertiesByLabel = (Boolean) config.getOrDefault( "nodePropsByLabel", false );

            try ( Transaction tx = db.beginTx() )
            {
                txDataMap.put( "transactionId", phase.equals( "after" ) ? txData.getTransactionId() : -1 );
                txDataMap.put( "commitTime", phase.equals( "after" ) ? txData.getCommitTime() : -1 );

                txDataMap.put( "createdNodes", createdNodeMap(txData, uidKey ) );
                txDataMap.put( "createdRelationships", createdRelationshipsMap( txData, uidKey ) );

                txDataMap.put( "deletedNodes", deletedNodeMap( txData, uidKey ) );
                txDataMap.put( "deletedRelationships", deletedRelationshipsMap( txData, uidKey ) );

                txDataMap.put( "assignedLabels", assignedLabelMap( txData, uidKey ) );
                txDataMap.put( "removedLabels", removedLabelMap( txData, uidKey ) );

                if (nodePropertiesByLabel)
                {
                    txDataMap.put( "assignedNodeProperties", assignedNodePropertyMapByLabel( txData, uidKey ) );
                    txDataMap.put( "removedNodeProperties", removedNodePropertyMapByLabel( txData, uidKey ) );
                }
                else{
                    txDataMap.put( "assignedNodeProperties", assignedNodePropertyMap( txData, uidKey ) );
                    txDataMap.put( "removedNodeProperties", removedNodePropertyMap( txData, uidKey ) );
                }


                txDataMap.put( "assignedRelationshipProperties", assignedRelationshipPropertyMap( txData, uidKey ) );
                txDataMap.put( "removedRelationshipProperties", removedRelationshipPropertyMap( txData, uidKey ) );

                tx.success();
            }

            return map("txData", txDataMap );
        }

        @Override
        public void afterCommit(TransactionData txData, Object state) {
            executeTriggers(txData, "after");
        }

        @Override
        public void afterRollback(TransactionData txData, Object state) {
            executeTriggers(txData, "rollback");
        }

    }

    private static Map<String, Object> txDataParams(TransactionData txData, String phase) {
        return map("transactionId", phase.equals("after") ? txData.getTransactionId() : -1,
                        "commitTime", phase.equals("after") ? txData.getCommitTime() : -1,
                        "createdNodes", txData.createdNodes(),
                        "createdRelationships", txData.createdRelationships(),
                        "deletedNodes", txData.deletedNodes(),
                        "deletedRelationships", txData.deletedRelationships(),
                        "removedLabels", aggregateLabels(txData.removedLabels()),
                        "removedNodeProperties", aggregatePropertyKeys(txData.removedNodeProperties(),true,true),
                        "removedRelationshipProperties", aggregatePropertyKeys(txData.removedRelationshipProperties(),false,true),
                        "assignedLabels", aggregateLabels(txData.assignedLabels()),
                        "assignedNodeProperties",aggregatePropertyKeys(txData.assignedNodeProperties(),true,false),
                        "assignedRelationshipProperties",aggregatePropertyKeys(txData.assignedRelationshipProperties(),false,false));
    }

    private static <T extends PropertyContainer> Map<String,List<Map<String,Object>>> aggregatePropertyKeys(Iterable<PropertyEntry<T>> entries, boolean nodes, boolean removed) {
        if (!entries.iterator().hasNext()) return Collections.emptyMap();
        Map<String,List<Map<String,Object>>> result = new HashMap<>();
        String entityType = nodes ? "node" : "relationship";
        for (PropertyEntry<T> entry : entries) {
            result.compute(entry.key(),
                    (k, v) -> {
                        if (v == null) v = new ArrayList<>(100);
                        Map<String, Object> map = map("key", k, entityType, entry.entity(), "old", entry.previouslyCommitedValue());
                        if (!removed) map.put("new", entry.value());
                        v.add(map);
                        return v;
                    });
        }
        return result;
    }
    private static Map<String,List<Node>> aggregateLabels(Iterable<LabelEntry> labelEntries) {
        if (!labelEntries.iterator().hasNext()) return Collections.emptyMap();
        Map<String,List<Node>> result = new HashMap<>();
        for (LabelEntry entry : labelEntries) {
            result.compute(entry.label().name(),
                    (k, v) -> {
                        if (v == null) v = new ArrayList<>(100);
                        v.add(entry.node());
                        return v;
                    });
        }
        return result;
    }

    public static class LifeCycle {
        private final GraphDatabaseAPI db;
        private final Log log;
        private TriggerHandler triggerHandler;

        public LifeCycle(GraphDatabaseAPI db, Log log) {
            this.db = db;
            this.log = log;
        }

        public void start() {
            boolean enabled = Util.toBoolean(ApocConfiguration.get("trigger.enabled", null));
            if (!enabled) return;
            triggerHandler = new Trigger.TriggerHandler(db,log);
            db.registerTransactionEventHandler(triggerHandler);
        }

        public void stop() {
            if (triggerHandler == null) return;
            db.unregisterTransactionEventHandler(triggerHandler);
        }
    }


    public enum ActionType
    {
        ADDED,
        REMOVED

    }

    public static final String ARRAY_PREFIX = "Array";

    public static Map<String,NodeChange> updatedNodeMap( TransactionData tx, Iterable<Node> updatedNodes, String uidKey, ActionType actionType )
    {
        Map<String,NodeChange> nodeChanges = new HashMap<>();

        Iterator<Node> updatedNodesIterator = updatedNodes.iterator();
        while ( updatedNodesIterator.hasNext() )
        {
            Node node = updatedNodesIterator.next();
            String nodeUid = Long.toString( node.getId() );

            switch ( actionType )
            {
            case ADDED:
                nodeUid = (String) node.getProperty( uidKey, Long.toString( node.getId() ) );
                break;
            case REMOVED:
                nodeUid = getNodeUidFromPreviousCommit( tx, uidKey, node.getId() );
                break;
            }

            nodeChanges.put( nodeUid, new NodeChange( actionType ) );
        }

        return nodeChanges.isEmpty() ? Collections.emptyMap() : nodeChanges;
    }

    public static Map<String,NodeChange> createdNodeMap( TransactionData tx, String uidKey )
    {
        return updatedNodeMap( tx, tx.createdNodes(), uidKey, ActionType.ADDED );
    }

    public static Map<String,NodeChange> deletedNodeMap( TransactionData tx, String uidKey )
    {
        return updatedNodeMap( tx, tx.deletedNodes(), uidKey, ActionType.REMOVED );
    }

    public static Map<String,RelationshipChange> updatedRelationshipsMap( TransactionData tx, Iterable<Relationship> updatedRelationships, String uidKey, ActionType actionType )
    {
        Map<String,RelationshipChange> relationshipChanges = new HashMap<>();

        Iterator<Relationship> updatedRelationshipsIterator = updatedRelationships.iterator();
        while ( updatedRelationshipsIterator.hasNext() )
        {
            Relationship relationship = updatedRelationshipsIterator.next();

            Node startNode = relationship.getStartNode();
            Node endNode = relationship.getEndNode();

            String relationshipUid = Long.toString( relationship.getId() );
            String startNodeUid = Long.toString( relationship.getId() );
            String endNodeUid = Long.toString( relationship.getId() );

            switch ( actionType )
            {
            case ADDED:
                relationshipUid = (String) relationship.getProperty( uidKey, Long.toString( relationship.getId() ) );
                startNodeUid = (String) startNode.getProperty( uidKey, Long.toString( startNode.getId() ) );
                endNodeUid = (String) endNode.getProperty( uidKey, Long.toString( endNode.getId() ) );
                break;
            case REMOVED:
                relationshipUid = getRelationshipUidFromPreviousCommit( tx, uidKey, relationship.getId() );
                startNodeUid = getRelationshipUidFromPreviousCommit( tx, uidKey, startNode.getId() );
                endNodeUid = getRelationshipUidFromPreviousCommit( tx, uidKey, endNode.getId() );
                break;
            }

            relationshipChanges.put( relationshipUid, new RelationshipChange( startNodeUid, endNodeUid, relationship.getType().name(), actionType ) );
        }

        return relationshipChanges.isEmpty() ? Collections.emptyMap() : relationshipChanges;
    }

    public static Map<String,RelationshipChange> createdRelationshipsMap( TransactionData tx, String uidKey )
    {
        return updatedRelationshipsMap( tx, tx.createdRelationships(), uidKey, ActionType.ADDED );
    }

    public static Map<String,RelationshipChange> deletedRelationshipsMap( TransactionData tx, String uidKey )
    {
        return updatedRelationshipsMap( tx, tx.deletedRelationships(), uidKey, ActionType.REMOVED );
    }



    public static Map<String,List<LabelChange>> updatedLabelMap( TransactionData tx, Iterable<LabelEntry> assignedLabels, String uidKey, ActionType actionType)
    {
        Map<String,List<LabelChange>> labelChanges = new HashMap<>(  );

        Iterator<LabelEntry> assignedLabelsIterator = assignedLabels.iterator();
        while ( assignedLabelsIterator.hasNext() )
        {
            LabelEntry labelEntry = assignedLabelsIterator.next();
            Node updatedNode = labelEntry.node();

            String label = labelEntry.label().name();
            String nodeUid = Long.toString( updatedNode.getId() );

            switch ( actionType )
            {
            case ADDED:
                nodeUid = (String) updatedNode.getProperty( uidKey, Long.toString( updatedNode.getId() ) );
                break;
            case REMOVED:
                nodeUid = getNodeUidFromPreviousCommit( tx, uidKey, updatedNode.getId());
                break;
            }

            if (!labelChanges.containsKey( label ))
            {
                labelChanges.put( label, new ArrayList<>(  ) );
            }
            labelChanges.get( label ).add( new LabelChange( nodeUid, actionType ) );
        }

        return labelChanges.isEmpty() ? Collections.emptyMap() : labelChanges;
    }

    public static Map<String,List<LabelChange>> assignedLabelMap( TransactionData tx, String uidKey )
    {
        return updatedLabelMap( tx, tx.assignedLabels(), uidKey, ActionType.ADDED );
    }

    public static Map<String,List<LabelChange>> removedLabelMap( TransactionData tx, String uidKey )
    {
        return updatedLabelMap( tx, tx.removedLabels(), uidKey, ActionType.REMOVED );
    }

    public static Map<String,Map<String,List<PropertyChange>>> updatedNodePropertyMapByLabel( TransactionData tx, Iterable<PropertyEntry<Node>> entityIterable, String uidKey, ActionType actionType, Function<Long,String> uidFunction )
    {
        Map<String,Map<String,List<PropertyChange>>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Node>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Node> entityPropertyEntry = entityIterator.next();

            Node updatedEntity = entityPropertyEntry.entity();
            String entityUid = Long.toString( updatedEntity.getId() );

            List<String> labels = new ArrayList<>(  );

            if ( actionType.equals( ActionType.REMOVED ) )
            {
                // Get the nodes where the node properties were removed
                Node node = ((Function<Long,Node>) (Long id) -> StreamSupport.stream( tx.removedNodeProperties().spliterator(), true )
                        .filter( nodePropertyEntry -> id.equals(  nodePropertyEntry.entity().getId()) ).reduce( (t1,t2) -> t1 ).get().entity()).apply( updatedEntity.getId() );

                // Check if the node has been deleted
                if ( StreamSupport.stream( tx.deletedNodes().spliterator(), false ).map( n -> n.getId() ).collect( Collectors.toList() ).contains(
                        node.getId() ) )
                {
                    // If deleted get the labels from the transaction data.
                    labels.addAll( StreamSupport.stream( tx.removedLabels().spliterator(), false ).filter(
                            labelEntry -> labelEntry.node().getId() == node.getId() ).map( labelEntry -> labelEntry.label().name() ).collect(
                            Collectors.toList() ) );
                    }
                    else
                    {
                        labels.addAll(
                                StreamSupport.stream( node.getLabels().spliterator(), true ).map( label -> label.name() ).collect( Collectors.toList() ) );
                    }

                entityUid = uidFunction.apply( updatedEntity.getId() );
            }
            else
            {
                labels = StreamSupport.stream( updatedEntity.getLabels().spliterator(), true ).map( Label::name ).collect( Collectors.toList() );
                entityUid = (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) );
            }

            for ( String label : labels )
            {
                if ( !propertyChanges.containsKey( label ) )
                {
                    propertyChanges.put( label, new HashMap<>() );
                }
                if ( !propertyChanges.get( label ).containsKey( entityUid ) )
                {
                    propertyChanges.get( label ).put( entityUid, new ArrayList<>() );
                }
                if (actionType.equals( ActionType.REMOVED ))
                {
                    propertyChanges.get( label ).get( entityUid ).add(
                            new PropertyChange( entityPropertyEntry.key(), null, entityPropertyEntry.previouslyCommitedValue(), actionType ) );
                }
                else
                {
                    propertyChanges.get( label ).get( entityUid ).add(
                            new PropertyChange( entityPropertyEntry.key(), entityPropertyEntry.value(), entityPropertyEntry.previouslyCommitedValue(),
                                    actionType ) );
                }
            }
        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }

    public static Map<String,Map<String,List<PropertyChange>>> assignedNodePropertyMapByLabel(TransactionData tx, String uidKey )
    {
        return updatedNodePropertyMapByLabel( tx, tx.assignedNodeProperties(), uidKey, ActionType.ADDED, (l) -> getNodeUidFromPreviousCommit( tx, uidKey, l ));
    }

    public static Map<String,Map<String,List<PropertyChange>>> removedNodePropertyMapByLabel(TransactionData tx, String uidKey )
    {
        return updatedNodePropertyMapByLabel( tx, tx.removedNodeProperties(), uidKey, ActionType.REMOVED, (l) -> getNodeUidFromPreviousCommit( tx, uidKey, l ));
    }


    public static <T extends Entity> Map<String,List<PropertyChange>> updatedEntityPropertyMap( TransactionData tx, Iterable<PropertyEntry<T>> entityIterable, String uidKey, ActionType actionType, Function<Long,String> uidFunction )
    {
        Map<String,List<PropertyChange>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<T>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<T> entityPropertyEntry = entityIterator.next();

            T updatedEntity = entityPropertyEntry.entity();
            String entityUid = (actionType.equals( ActionType.REMOVED ) ? uidFunction.apply( updatedEntity.getId() ) : (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) ));

            if (!propertyChanges.containsKey( entityUid ))
            {
                propertyChanges.put(entityUid, new ArrayList<>(  ) );

            }
            if (actionType.equals( ActionType.REMOVED ))
            {
                propertyChanges.get( entityUid ).add(
                        new PropertyChange( entityPropertyEntry.key(), null, entityPropertyEntry.previouslyCommitedValue(), actionType ) );
            }
            else
            {
                propertyChanges.get( entityUid ).add(
                        new PropertyChange( entityPropertyEntry.key(), entityPropertyEntry.value(), entityPropertyEntry.previouslyCommitedValue(), actionType ) );
            }

        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }
    
    public static <T extends Entity> Map<String,List<PropertyChange>> assignedNodePropertyMap(TransactionData tx, String uidKey )
    {
        return updatedEntityPropertyMap( tx, tx.assignedNodeProperties(), uidKey, ActionType.ADDED, (l) -> getNodeUidFromPreviousCommit( tx, uidKey, l ));
    }

    public static <T extends Entity> Map<String,List<PropertyChange>> assignedRelationshipPropertyMap(TransactionData tx, String uidKey )
    {
        return updatedEntityPropertyMap( tx, tx.assignedRelationshipProperties(), uidKey, ActionType.ADDED, (l) -> getRelationshipUidFromPreviousCommit( tx, uidKey, l ));
    }

    public static <T extends Entity> Map<String,List<PropertyChange>> removedNodePropertyMap(TransactionData tx, String uidKey )
    {
        return updatedEntityPropertyMap( tx, tx.removedNodeProperties(), uidKey, ActionType.REMOVED, (l) -> getNodeUidFromPreviousCommit( tx, uidKey, l ));
    }

    public static <T extends Entity> Map<String,List<PropertyChange>> removedRelationshipPropertyMap(TransactionData tx, String uidKey )
    {
        return updatedEntityPropertyMap( tx, tx.removedRelationshipProperties(), uidKey, ActionType.REMOVED, (l) -> getRelationshipUidFromPreviousCommit( tx, uidKey, l ));
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class LabelChange
    {
        private String nodeUid;

        private ActionType action;

        public LabelChange( String nodeUid, ActionType action )
        {
            this.nodeUid = nodeUid;
            this.action = action;
        }

        public String getNodeUid()
        {
            return nodeUid;
        }

        public void setNodeUid( String nodeUid )
        {
            this.nodeUid = nodeUid;
        }

        public ActionType getAction()
        {
            return action;
        }

        public void setAction( ActionType action )
        {
            this.action = action;
        }
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class NodeChange
    {
        private ActionType action;

        public NodeChange( ActionType action )
        {
            this.action = action;
        }

        public ActionType getAction()
        {
            return action;
        }

        public void setAction( ActionType action )
        {
            this.action = action;
        }
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class RelationshipChange
    {
        private String uidOfStartNode;

        private String uidOfEndNode;

        private String type;

        private ActionType action;

        public RelationshipChange( String uidOfStartNode, String uidOfEndNode, String type, ActionType action )
        {
            this.uidOfStartNode = uidOfStartNode;
            this.uidOfEndNode = uidOfEndNode;
            this.type = type;
            this.action = action;
        }

        public String getUidOfStartNode()
        {
            return uidOfStartNode;
        }

        public void setUidOfStartNode( String uidOfStartNode )
        {
            this.uidOfStartNode = uidOfStartNode;
        }

        public String getUidOfEndNode()
        {
            return uidOfEndNode;
        }

        public void setUidOfEndNode( String uidOfEndNode )
        {
            this.uidOfEndNode = uidOfEndNode;
        }

        public String getType()
        {
            return type;
        }

        public void setType( String type )
        {
            this.type = type;
        }

        public ActionType getAction()
        {
            return action;
        }

        public void setAction( ActionType action )
        {
            this.action = action;
        }
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class PropertyChange
    {
        private String key;

        private Object value;

        private Object oldValue;

        private String type;

        private ActionType action;

        PropertyChange( String key, Object value, Object oldValue, ActionType action)
        {
            this.key = key;
            this.value = value;
            this.type = value == null ? null : value.getClass().getSimpleName();
            if (type != null)
            {
                if ( value instanceof long[] )
                {
                    this.type = ARRAY_PREFIX + ".Long";
                }
                else if ( value instanceof boolean[] )
                {
                    this.type = ARRAY_PREFIX + ".Boolean";
                }
                else if ( value instanceof Object[] )
                {
                    this.type = ARRAY_PREFIX + "." + (((Object[]) value)[0]).getClass().getSimpleName();
                }
            }
            this.action = action;
            this.oldValue = oldValue;
        }

        public String getKey()
        {
            return key;
        }

        public void setKey( String key )
        {
            this.key = key;
        }

        public Object getValue()
        {
            return value;
        }

        public void setValue( Object value )
        {
            this.value = value;
        }

        public Object getOldValue()
        {
            return oldValue;
        }

        public void setOldValue( Object oldValue )
        {
            this.oldValue = oldValue;
        }

        public String getType()
        {
            return type;
        }

        public void setType( String type )
        {
            this.type = type;
        }

        public ActionType getAction()
        {
            return action;
        }

        public void setAction( ActionType action )
        {
            this.action = action;
        }
    }

    public static String getNodeUidFromPreviousCommit(TransactionData tx, String uidKey, Long id)
    {
        String result = StreamSupport.stream( tx.removedNodeProperties().spliterator(), true )
                .filter( (p) -> p.key().equals( uidKey ) )
                .filter( (p) -> p.entity().getId() == id )
                .map( (p) -> (String) p.previouslyCommitedValue() )
                .collect( Collectors.joining());

        return (result.isEmpty()) ? Long.toString( id ) : result;
    };

    public static String getRelationshipUidFromPreviousCommit(TransactionData tx, String uidKey, Long id)
    {
        String result = StreamSupport.stream( tx.removedRelationshipProperties().spliterator(), true )
                .filter( (p) -> p.key().equals( uidKey ) )
                .filter( (p) -> p.entity().getId() == id )
                .map( (p) -> (String) p.previouslyCommitedValue() )
                .collect( Collectors.joining());

        return (result.isEmpty()) ? Long.toString( id ) : result;
    };
}
