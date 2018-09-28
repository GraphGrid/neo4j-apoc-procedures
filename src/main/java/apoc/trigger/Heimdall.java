package apoc.trigger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.TransactionData;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Heimdall
{

//
//    private static final String TX_ID = "txId";
//    private static final String TX_COMMIT_TIME = "txCommitTime";
//    private static final String NODE_CHANGES = "nodeChanges";
//    private static final String PROPERTY_CHANGES = "propertyChanges";
//    private static final String LABEL_CHANGES = "labelChanges";
//    private static final String RELATIONSHIP_CHANGES = "relationshipChanges";
//
//    static JsonFactory jsonFactory = new JsonFactory(  );
//
//
//    private List<NodeChange> nodeChanges = new ArrayList<>();
//
//    private List<PropertyChange> propertyChanges = new ArrayList<>();
//
//    private List<LabelChange> labelChanges = new ArrayList<>();
//
//    private List<RelationshipChange> relationshipChanges = new ArrayList<>();
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
//
//    private void addNodeChanges(Iterable<Node> nodeIter, Map<Long,String> nodeIdToGrn, ActionType actionType)
//    {
//        for ( Node node : nodeIter)
//        {
//            if (actionType == ActionType.added)
//            {
//                this.validateAndHandleNode( node );
//                NodeChange nodeChange = new NodeChange( node.getProperty( idKey ).toString(), actionType );
//                nodeChanges.add( nodeChange );
//            }
//            else
//            {
//                long id = node.getId();
//                String grn = nodeIdToGrn.get(id);
//                if (grn==null)
//                {
//                    continue;
//                }
//                NodeChange nodeChange = new NodeChange(grn, actionType );
//                nodeChanges.add( nodeChange );
//            }
//        }
//    }
//
//    private void addLabelChanges(Iterable<LabelEntry> nodeIter, Map<Long,String> nodeIdToGrn, ActionType actionType)
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
//                this.validateAndHandleNode( label.node() );
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
//    private void addRelationshipChanges(Iterable<Relationship> relIter, Map<Long,String> nodeIdToGrn,
//            Map<Long,String> relIdToGrn,
//            ActionType actionType)
//    {
//        for ( Relationship rel : relIter)
//        {
//            Node startNode = rel.getStartNode();
//            Node endNode = rel.getEndNode();
//            if (ActionType.added == actionType)
//            {
//                this.validateAndHandleRelationship(startNode, endNode, rel);
//            }
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
//                    actionType);
//            relationshipChanges.add( relChange );
//        }
//    }
//
//    public static class RelationshipChange
//    {
//
//        private String grnOfStartNode;
//
//        private String grnOfEndNode;
//
//        private String type;
//
//        private ActionType action;
//
//        RelationshipChange(String grnOfStartNode, String grnOfEndNode, String type,
//                ActionType action)
//        {
//            this.grnOfStartNode = grnOfStartNode;
//            this.grnOfEndNode = grnOfEndNode;
//            this.type = type;
//            this.action = action;
//        }
//
//        public String getGrnOfStartNode()
//        {
//            return grnOfStartNode;
//        }
//
//        public String getGrnOfEndNode()
//        {
//            return grnOfEndNode;
//        }
//
//        public ActionType getAction()
//        {
//            return action;
//        }
//    }
//
//    public enum ActionType
//    {
//        removed,
//        added
//    }


}
