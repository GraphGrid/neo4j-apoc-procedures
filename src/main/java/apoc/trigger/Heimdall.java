package apoc.trigger;

import apoc.ApocConfiguration;
import apoc.Description;
import apoc.coll.SetBackedList;
import apoc.util.Util;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static apoc.util.Util.map;

public class Heimdall
{

//    protected List<NodeChange> nodeChanges = new ArrayList<>();

//    private List<PropertyChange> propertyChanges = new ArrayList<>();
//
//    private List<LabelChange> labelChanges = new ArrayList<>();
//
//    private List<RelationshipChange> relationshipChanges = new ArrayList<>();
//

    public static class TriggerInfo {
        public String name;
        public String query;
        public Map<String,Object> selector;
        public Map<String, Object> params;
        public boolean installed;
        public boolean paused;
        public boolean txDataPayload;

        public TriggerInfo(String name, String query, Map<String, Object> selector, boolean installed, boolean paused) {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.installed = installed;
            this.paused = paused;
        }

        public TriggerInfo( String name, String query, Map<String,Object> selector, Map<String,Object> params, boolean installed, boolean paused )
        {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.params = params;
            this.installed = installed;
            this.paused = paused;
        }

        public TriggerInfo( String name, String query, Map<String,Object> selector, Map<String,Object> params, boolean installed, boolean paused,
                boolean txDataPayload )
        {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.params = params;
            this.installed = installed;
            this.paused = paused;
            this.txDataPayload = txDataPayload;
        }
    }

    @Context
    public GraphDatabaseService db;

    @UserFunction(value = "apoc.heimdall.nodesByLabel")
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

    @UserFunction(value = "apoc.heimdall.propertiesByKey")
    @Description("function to filter propertyEntries by property-key, to be used within a trigger statement with {assignedNode/RelationshipProperties} and {removedNode/RelationshipProperties}. Returns [{old,new,key,node,relationship}]")
    public List<Map<String,Object>> propertiesByKey(@Name("propertyEntries") Map<String,List<Map<String,Object>>> propertyEntries, @Name("key") String key) {
        return propertyEntries.getOrDefault(key,Collections.emptyList());
    }

    private static String statement(Map<String,Object> map) {
        return (String)map.getOrDefault("statement",map.get("kernelTransaction"));
    }

    @Procedure(mode = Mode.WRITE, value = "apoc.heimdall.add")
    @Description("add a trigger statement under a name, in the statement you can use {createdNodes}, {deletedNodes} etc., the selector is {phase:'before/after/rollback'} returns previous and new trigger information. Takes in an optional configuration.")
    public Stream<Trigger.TriggerInfo> add(@Name("name") String name, @Name("statement") String statement, @Name(value = "selector"/*, defaultValue = "{}"*/)  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config, @Name(value = "txDataPayload", defaultValue = "false") boolean txDataPayload) {
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = Trigger.TriggerHandler.add(name, statement, selector, params, txDataPayload);
        if (removed != null) {
            return Stream.of(
                    new Trigger.TriggerInfo(name,statement(removed), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false, txDataPayload),
                    new Trigger.TriggerInfo(name,statement,selector, params,true, false, txDataPayload));
        }
        return Stream.of(new Trigger.TriggerInfo(name,statement,selector, params,true, false,txDataPayload));
    }

    @Procedure(mode = Mode.WRITE, value = "apoc.heimdall.remove")
    @Description("remove previously added trigger, returns trigger information")
    public Stream<Trigger.TriggerInfo> remove(@Name("name")String name) {
        Map<String, Object> removed = Trigger.TriggerHandler.remove(name);
        if (removed == null) {
            Stream.of(new Trigger.TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new Trigger.TriggerInfo(name,statement(removed), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false, (boolean) removed.get("txDataPayload")));
    }

    @PerformsWrites
    @Procedure(value = "apoc.heimdall.list")
    @Description("list all installed triggers")
    public Stream<Trigger.TriggerInfo> list() {
        return Trigger.TriggerHandler.list().entrySet().stream()
                .map( (e) -> new Trigger.TriggerInfo(e.getKey(),statement(e.getValue()),(Map<String,Object>)e.getValue().get("selector"), (Map<String, Object>) e.getValue().get("params"),true, (Boolean) e.getValue().get("paused"), (boolean) e.getValue().get("txDataPayload")));
    }

    @Procedure(mode = Mode.WRITE, value = "apoc.heimdall.pause")
    @Description("CALL apoc.trigger.pause(name) | it pauses the trigger")
    public Stream<Trigger.TriggerInfo> pause(@Name("name")String name) {
        Map<String, Object> paused = Trigger.TriggerHandler.paused(name);
        return Stream.of(new Trigger.TriggerInfo(name,statement(paused), (Map<String,Object>) paused.get("selector"), (Map<String,Object>) paused.get("params"),true, true, (boolean) paused.get("txDataPayload")));
    }

    @Procedure(mode = Mode.WRITE, value = "apoc.heimdall.resume")
    @Description("CALL apoc.trigger.resume(name) | it resumes the paused trigger")
    public Stream<Trigger.TriggerInfo> resume(@Name("name")String name) {
        Map<String, Object> resume = Trigger.TriggerHandler.resume(name);
        return Stream.of(new Trigger.TriggerInfo(name,statement(resume), (Map<String,Object>) resume.get("selector"), (Map<String,Object>) resume.get("params"),true, false, (boolean) resume.get("txDataPayload")));
    }

    public static class HeimdallHandler implements TransactionEventHandler
    {
        public static final String APOC_HEIMDALL = "apoc.heimdall";
        static ConcurrentHashMap<String,Map<String,Object>> triggers = new ConcurrentHashMap(map("",map()));
        private static GraphProperties properties;
        private final Log log;
        public HeimdallHandler( GraphDatabaseAPI api, Log log) {
            properties = api.getDependencyResolver().resolveDependency( NodeManager.class).newGraphProperties();
//            Pools.SCHEDULED.submit(() -> updateTriggers(null,null));
            this.log = log;
        }

        public static Map<String, Object> add(String name, String statement, Map<String,Object> selector) {
            return add(name, statement, selector, Collections.emptyMap(), false);
        }

        public static Map<String, Object> add(String name, String statement, Map<String,Object> selector, Map<String,Object> params, boolean txDataPayload) {
            return updateTriggers(name, map("statement", statement, "selector", selector, "params", params, "paused", false, "txDataPayload", txDataPayload));
        }
        public synchronized static Map<String, Object> remove(String name) {
            return updateTriggers(name,null);
        }

        public static Map<String, Object> paused(String name) {
            Map<String, Object> triggerToPause = triggers.get(name);
            updateTriggers(name, map("statement", triggerToPause.get("statement"), "selector", triggerToPause.get("selector"), "params", triggerToPause.get("params"), "paused", true, "txDataPayload", triggerToPause.get( "txDataPayload" )));
            return triggers.get(name);
        }

        public static Map<String, Object> resume(String name) {
            Map<String, Object> triggerToResume = triggers.get(name);
            updateTriggers(name, map("statement", triggerToResume.get("statement"), "selector", triggerToResume.get("selector"), "params", triggerToResume.get("params"), "paused", false, "txDataPayload", triggerToResume.get( "txDataPayload" )));
            return triggers.get(name);
        }

        private synchronized static Map<String, Object> updateTriggers(String name, Map<String, Object> value) {
            try ( Transaction tx = properties.getGraphDatabase().beginTx()) {
                triggers.clear();
                String triggerProperty = (String) properties.getProperty(APOC_HEIMDALL, "{}");
                triggers.putAll( Util.fromJson(triggerProperty,Map.class));
                Map<String,Object> previous = null;
                if (name != null) {
                    previous = (value == null) ? triggers.remove(name) : triggers.put(name, value);
                    if (value != null || previous != null) {
                        properties.setProperty(APOC_HEIMDALL, Util.toJson(triggers));
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
            Map<String, Object> params = txDataParams(txData, phase, false);
            triggers.forEach((name, data) -> {
                if( data.get("paused").equals(false)) {
                    if( data.get( "params" ) != null)
                    {
                        params.putAll( (Map<String,Object>) data.get( "params" ) );
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

        @Override
        public void afterCommit(TransactionData txData, Object state) {
            executeTriggers(txData, "after");
        }

        @Override
        public void afterRollback(TransactionData txData, Object state) {
            executeTriggers(txData, "rollback");
        }

    }

    private static Map<String, Object> txDataParams(TransactionData txData, String phase, boolean txDataPayload) {
        List<NodeChange> nodeChanges = new ArrayList<>();

        List<PropertyChange> propertyChanges = new ArrayList<>();

        List<LabelChange> labelChanges = new ArrayList<>();

        List<RelationshipChange> relationshipChanges = new ArrayList<>();


        nodeChanges = getNodeChanges(txData.createdNodes(), ActionType.added, "1");

        Map<String,Object> txMap
                = map("transactionId", phase.equals("after") ? txData.getTransactionId() : -1,
                "commitTime", phase.equals("after") ? txData.getCommitTime() : -1,
//                "createdNodes", txData.createdNodes(),
                "createdNodes", nodeChanges,
                "createdRelationships", txData.createdRelationships(),
                "deletedNodes", txData.deletedNodes(),
                "deletedRelationships", txData.deletedRelationships(),
                "removedLabels", aggregateLabels(txData.removedLabels()),
                "removedNodeProperties", aggregatePropertyKeys(txData.removedNodeProperties(),true,true),
                "removedRelationshipProperties", aggregatePropertyKeys(txData.removedRelationshipProperties(),false,true),
                "assignedLabels", aggregateLabels(txData.assignedLabels()),
                "assignedNodeProperties",aggregatePropertyKeys(txData.assignedNodeProperties(),true,false),
                "assignedRelationshipProperties",aggregatePropertyKeys(txData.assignedRelationshipProperties(),false,false));
        return map( "txData", txMap);

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
        private Heimdall.HeimdallHandler heimdallHandler;

        public LifeCycle(GraphDatabaseAPI db, Log log) {
            this.db = db;
            this.log = log;
        }

        public void start() {
            boolean enabled = Util.toBoolean( ApocConfiguration.get("trigger.enabled", null));
            if (!enabled) return;
            heimdallHandler = new Heimdall.HeimdallHandler( db,log);
            db.registerTransactionEventHandler(heimdallHandler);
        }

        public void stop() {
            if (heimdallHandler== null) return;
            db.unregisterTransactionEventHandler(heimdallHandler);
        }

    }

    public static class TransactionDataMap
    {

        private static final String TX_ID = "txId";
        private static final String TX_COMMIT_TIME = "txCommitTime";
        private static final String NODE_CHANGES = "nodeChanges";
        private static final String PROPERTY_CHANGES = "propertyChanges";
        private static final String LABEL_CHANGES = "labelChanges";
        private static final String RELATIONSHIP_CHANGES = "relationshipChanges";

        private Map<String,NodeChange> nodeChangeMap;


        public static abstract class EntityChange {

            public String entityId;

        }

        public static class NodeChange extends EntityChange
        {

            private List<String> createdNodes;
            private List<String> deletedNodes;
            private Map<String, PropertyChange> updatedNodes;

        }

    }

//

//    static JsonFactory jsonFactory = new JsonFactory(  );
//
//

//
//    public Map<String,Object> txDataToJson( TransactionData transactionData )
//    {
//
//        JsonGenerator jsonGenerator = new JsonFactory( ).createGenerator(  );
//
//        transactionData.createdNodes();
//
//
//        Map<String,Object> result = new HashMap<>(  );
//
//
//
//        return result;
//    }

    public enum ActionType
    {
        removed,
        added
    }


    public static final String NODES_ADDED = "Nodes_Added";
    public static final String NODES_REMOVED = "Nodes_Removed";
    public static final String LABELS_ADDED = "Labels_Added";
    public static final String LABELS_REMOVED = "Labels_Removed";
    public static final String RELATIONSHIPS_ADDED = "Relationships_Added";
    public static final String RELATIONSHIPS_REMOVED = "Relationships_Removed";
    public static final String PROPERTIES_ADDED = "Properties_Added";
    public static final String PROPERTIES_REMOVED = "Properties_Removed";
    public static final String ARRAY_PREFIX = "Array";

    public static class LabelChange
    {
        private String grn;

        private String name;

        private ActionType action;

        LabelChange( String grn, String name, ActionType action )
        {
            this.grn = grn;
            this.name = name;
            this.action = action;
        }

        public String getGrn()
        {
            return grn;
        }

        public ActionType getAction()
        {
            return action;
        }

        @Override
        public String toString()
        {
            return this.grn + ":" + this.name;
        }
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class NodeChange
    {

        private String id;

        private ActionType action;

        public NodeChange( String id, ActionType action )
        {
            this.action = action;
            this.id = id;
        }

        public String getId()
        {
            return id;
        }

        public ActionType getAction()
        {
            return action;
        }

        @Override
        public String toString()
        {
            return this.id;
        }
    }

    public class PropertyChange
    {
        private RelationshipChange relChange;

        private String grn;

        private String key;

        private Object value;

        private Object oldValue;

        private String type;

        private ActionType action;

        private List<int[]> stringArrayValueLocationList = new ArrayList<>(  );

        private List<int[]> grnStartAndStop = new ArrayList<>(  );

        private int[] varNameStartAndStop = new int[2];

        private String fmtMessage;

        PropertyChange(String grn, String key)
        {
            this.grn = grn;
            this.key = key;
            this.action = ActionType.removed;
        }

        PropertyChange(String grnOfStartNode, String grnOfEndNode, String type, String grnOfRel, String key)
        {
            // If the property change is a relationship property set a start and end node but not a grn
            this.relChange = new RelationshipChange( grnOfStartNode, grnOfEndNode, type, grnOfRel, ActionType.removed);
            this.key = key;
            this.action = ActionType.removed;
        }

        PropertyChange(String grn, String key, Object value, Object oldValue, ActionType action)
        {
            this.grn = grn;
            this.key = key;
            this.value = value;
            this.type = value.getClass().getSimpleName();
            if (value instanceof long[])
            {
                this.type = ARRAY_PREFIX+".Long";
            }
            else if (value instanceof boolean[])
            {
                this.type = ARRAY_PREFIX+".Boolean";
            }
            else if (value instanceof Object[])
            {
                this.type = ARRAY_PREFIX+"."+(((Object[])value)[0]).getClass().getSimpleName();
            }
            this.action = action;
            this.oldValue = oldValue;
        }

        PropertyChange(String grnOfStartNode, String grnOfEndNode, String relType, String grnOfRel, String key, Object
                value, Object oldValue, ActionType action)
        {
            this.relChange = new RelationshipChange( grnOfStartNode, grnOfEndNode, relType, grnOfRel,
                    ActionType.removed);
            this.key = key;
            this.value = value;
            this.type = value.getClass().getSimpleName();
            if (value instanceof long[])
            {
                this.type = ARRAY_PREFIX+".Long";
            }
            else if (value instanceof boolean[])
            {
                this.type = ARRAY_PREFIX+".Boolean";
            }
            else if (value instanceof Object[])
            {
                this.type = ARRAY_PREFIX+"."+(((Object[])value)[0]).getClass().getSimpleName();
            }
            this.action = action;
            this.oldValue = oldValue;
        }

        @Override
        public String toString()
        {
            return this.fmtMessage;
        }
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class RelationshipChange
    {

        private String grnOfStartNode;

        private String grnOfEndNode;

        private String type;

        private String grnOfRelationship;

        private ActionType action;

        private String finalMsg;

        RelationshipChange(String grnOfStartNode, String grnOfEndNode, String type, String grnOfRelationship,
                ActionType action)
        {
            this.grnOfStartNode = grnOfStartNode;
            this.grnOfEndNode = grnOfEndNode;
            this.type = type;
            this.grnOfRelationship = grnOfRelationship;
            this.action = action;
        }

        public String getGrnOfStartNode()
        {
            return grnOfStartNode;
        }

        public void setGrnOfStartNode( String grnOfStartNode )
        {
            this.grnOfStartNode = grnOfStartNode;
        }

        public String getGrnOfEndNode()
        {
            return grnOfEndNode;
        }

        public void setGrnOfEndNode( String grnOfEndNode )
        {
            this.grnOfEndNode = grnOfEndNode;
        }

        public String getType()
        {
            return type;
        }

        public void setType( String type )
        {
            this.type = type;
        }

        public String getGrnOfRelationship()
        {
            return grnOfRelationship;
        }

        public void setGrnOfRelationship( String grnOfRelationship )
        {
            this.grnOfRelationship = grnOfRelationship;
        }

        public ActionType getAction()
        {
            return action;
        }

        public void setAction( ActionType action )
        {
            this.action = action;
        }

        public String getFinalMsg()
        {
            return finalMsg;
        }

        public void setFinalMsg( String finalMsg )
        {
            this.finalMsg = finalMsg;
        }

        @Override
        public String toString()
        {
            return this.finalMsg;
        }
    }

    public static List<NodeChange> getNodeChanges( Iterable<Node> nodeIter, ActionType actionType, String idKey )
    {
        List<NodeChange> temp = new ArrayList<>();
        for ( Node node : nodeIter )
        {
            if ( actionType == ActionType.added )
            {
                temp.add( new NodeChange( node.getProperty( idKey ).toString(), actionType ) );
            }
            else
            {
                temp.add( new NodeChange( Long.toString( node.getId() ), actionType ) );
            }
        }
        return temp;
    }
//
//    public void addLabelChanges(Iterable<LabelEntry> nodeIter, Map<Long,String> nodeIdToGrn, ActionType actionType, String idKey)
//    {
//        for ( LabelEntry label : nodeIter)
//        {
//            // SKIP GRAPH RESOURCE
//            if (label.label().name().equals( "GraphResource" ))
//            {
//                continue;
//            }
//
//            if (actionType.equals( ActionType.added ))
//            {
//                LabelChange labelChange =
//                        new LabelChange( label.node().getProperty( idKey ).toString(), label.label().name(), actionType );
//                labelChanges.add( labelChange );
//            }
//            else
//            {
//                long nodeId = label.node().getId();
//                String grn = nodeIdToGrn.get(nodeId);
//                try
//                {
//                    grn = label.node().getProperty( idKey ).toString();
//                }catch (Exception e )
//                {
//                    // Do nothing
//                }
//                if (grn==null)
//                {
//                    continue;
//                }
//                LabelChange labelChange =
//                        new LabelChange(grn, label.label().name(), actionType );
//                labelChanges.add( labelChange );
//            }
//        }
//    }
//
//    public void addRelationshipChanges(Iterable<Relationship> relIter, Map<Long,String> nodeIdToGrn,
//            Map<Long,String> relIdToGrn,
//            ActionType actionType, String idKey)
//    {
//        for ( Relationship rel : relIter)
//        {
//            Node startNode = rel.getStartNode();
//            Node endNode = rel.getEndNode();
//            long startNodeId = startNode.getId();
//            long endNodeId = endNode.getId();
//            long relId = rel.getId();
//            String startGrn = "";
//            try
//            {
//                startGrn = startNode.getProperty( idKey ).toString();
//            }
//            catch ( Exception e )
//            {
//                startGrn = nodeIdToGrn.get( startNodeId );
//            }
//            String endGrn = "";
//            try
//            {
//                endGrn = endNode.getProperty( idKey ).toString();
//            }catch ( Exception e )
//            {
//                endGrn = nodeIdToGrn.get( endNodeId );
//            }
//            String relGrn = "";
//            try
//            {
//                relGrn = rel.getProperty( idKey ).toString();
//            }catch ( Exception e )
//            {
//                relGrn = relIdToGrn.get(relId);
//            }
//            String relType = rel.getType().name();
//            boolean cond1 = endGrn == null;
//            boolean cond2 = relGrn == null;
//            boolean cond3 = startGrn == null;
//            boolean cond4 = relType == null;
//            if (cond1||cond2||cond3||cond4)
//            {
//                continue;
//            }
//            RelationshipChange relChange = new RelationshipChange(startGrn,
//                    endGrn,
//                    relType,
//                    relGrn,
//                    actionType);
//            relationshipChanges.add( relChange );
//        }
//    }



}
