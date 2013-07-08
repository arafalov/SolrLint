package solrlint.cli;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.w3c.dom.*;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: arafalov
 * Date: 13-07-06
 * Time: 6:08 PM
 */
public class Main {

//    private static final String DB_PATH = "target/neo4j-lint-db";
    private static final String DB_PATH = "/Users/arafalov/projects/Graph2Site/Neo4J/neo4j-advanced-2.0.0-M03/data/graph.db";
//    private static final String SITE_PATH = "target/sites/lol/";

    private static enum MyRelTypes implements RelationshipType {DEFINES, REFERS};

//    private static String PROP_TITLE = "Title";
//    private static String PROP_URL = "URL";
//    private static String PROP_FILE_NAME = "FileName";

    static GraphDatabaseService graphDb;

    public static void main(String[] args) throws Exception {
        File schemaXMLFile = new File(args[0]);
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setIgnoringComments(true);
        documentBuilderFactory.setCoalescing(true);
//        documentBuilderFactory.setIgnoringElementContentWhitespace(true);
//        documentBuilderFactory.setValidating(true);

        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document doc = documentBuilder.parse(schemaXMLFile);

        Element root = doc.getDocumentElement();

        deleteDB();
        createDB();

        Transaction tx = graphDb.beginTx();
        printElementRecursively(root, 0, null);
        createLinks();
        tx.success();

        tx.finish();
    }

    private static void createLinks() {
        ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.DEV_NULL);

        StringBuffer qft = new StringBuffer();
        qft
                .append("start")
                .append(" f=node:node_auto_index('_Element:field OR _Element:dynamicField'),")
                        //NOTICE: Colons, not equals as we are using Lucene QUERY now
                .append(" t=node:node_auto_index(_Element = 'fieldType')") //Notice: Here it is equal, so a simpler search
                .append(" where")
                .append(" f.type=t.name")
                .append(" return")
                .append(" f, t");
//                .append(" f.name, f.type, t.class, f, t");
         String query = engine.prettify(qft.toString());
        System.out.println("Query: " + query);

        ExecutionResult result = engine.execute(query);
//        String resultText = result.dumpToString();
//        System.out.println("Result: \n" + resultText);
        ResourceIterator<Map<String, Object>> resultIter = result.javaIterator();
        Map<String, Object> singleResult;
        int fieldRefCount = 0;
        while (resultIter.hasNext())
        {
            singleResult = resultIter.next();
            org.neo4j.graphdb.Node field = (org.neo4j.graphdb.Node)singleResult.get("f");
            org.neo4j.graphdb.Node type = (org.neo4j.graphdb.Node) singleResult.get("t");
            field.createRelationshipTo(type, MyRelTypes.REFERS);
            fieldRefCount++;
        }
        System.out.printf("Found %d fields with valid references\n", fieldRefCount);

        String problemQuery =
                new StringBuffer()
                .append("START f=node:node_auto_index('_Element:field OR _Element:dynamicField')")
                .append(" MATCH f-[r?:REFERS]->t")
                .append(" WHERE r is null")
                .append(" RETURN f")
                .toString();
        ExecutionResult problemsRes = engine.execute(problemQuery);
        resultIter = problemsRes.javaIterator();
        System.out.println();
        while(resultIter.hasNext())
        {
            singleResult = resultIter.next();
            org.neo4j.graphdb.Node field = (org.neo4j.graphdb.Node)singleResult.get("f");
            System.out.printf("Problem with finding relevant type for %s '%s' of expected type '%s'\n",
                    field.getProperty("_Element"),
                    field.getProperty("name"),
                    field.getProperty("type"));
        }

        /*
        Look for missing field type definitions:
            START f=node:node_auto_index('_Element:field OR _Element:dynamicField')
            MATCH f-[r?:REFERS]->t
            WHERE r is null
            return f._Element, f.name, f.type, t.class
        */

    }

    private static String SPACES = new String("                                                                         ");

    public static void printElementRecursively(Node el, int offset, org.neo4j.graphdb.Node graphParent) {
        String value = el.getNodeValue();
        if (el instanceof Text) {
            value = value.trim();
            if (value.isEmpty()) return; //empty text node

            //We have text
            graphParent.setProperty("_Value", value);
        } else if (!(el instanceof Element)) {
            System.out.println("=========UNKNOWN ELEMENT=========");
        }

        //we have Element
        org.neo4j.graphdb.Node thisGraphElement = graphDb.createNode();
        if (graphParent != null)
        {
            graphParent.createRelationshipTo(thisGraphElement, MyRelTypes.DEFINES);
        }
        thisGraphElement.setProperty("_Element", el.getNodeName()); //TODO: Lowercase?
//        System.out.printf("%s%s (%s)", SPACES.substring(0, offset), el.getNodeName(), el.getNodeValue());
        NamedNodeMap attributes = el.getAttributes();
        if (attributes != null) {
            int attribCount = attributes.getLength();

            for (int i = 0; i < attribCount; i++) {
                Node attrib = attributes.item(i);
                thisGraphElement.setProperty(attrib.getNodeName(), attrib.getNodeValue());
//                System.out.printf(" %s = '%s'", attrib.getNodeName(), attrib.getNodeValue());
            }
        }
//        System.out.println();
        NodeList children = el.getChildNodes();
        int childCount = children.getLength();
        if (childCount == 0) return;

        //spit the children out
        offset += 2;
        for (int i = 0; i < childCount; i++) {
            printElementRecursively(children.item(i), offset, thisGraphElement);
        }
    }


    private static void deleteDB() {
        try {
            FileUtils.deleteRecursively(new File(DB_PATH));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static void createDB() {
        graphDb = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(DB_PATH)
                .setConfig( GraphDatabaseSettings.node_keys_indexable, "_Element,name" )
//                .setConfig( GraphDatabaseSettings.relationship_keys_indexable, "relProp1,relProp2" )
                .setConfig( GraphDatabaseSettings.node_auto_indexing, "true" )
//                .setConfig( GraphDatabaseSettings.relationship_auto_indexing, "true" )
                .newGraphDatabase();
        registerShutdownHook(graphDb);
        System.out.println("Database created/connected");
    }


    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        //Registers a shutdown hook for the Neo4J instance to shut down nicely o Ctrl-C

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

}
