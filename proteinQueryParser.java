import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterators;
import java.io.*;
import java.util.*;

public class proteinQueryParser {
    public static void createQuery() throws IOException {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("C:/Protein"));
        // query folder path
        String queryPath = "C:\\Users\\vatsa\\Documents\\Semester 4\\Graphs" +
                "\\Assignment 4\\Proteins\\Proteins\\part3_Proteins\\Proteins\\query";
        // target folder path
        String targetPath = "C:\\Users\\vatsa\\Documents\\Semester 4\\Graphs" +
                "\\Assignment 4\\Proteins\\Proteins\\part3_Proteins\\Proteins\\target";
        File queryFolder = new File(queryPath);
        File targetFolder = new File(targetPath);
        File[] listOfQueryFiles = queryFolder.listFiles();
        File[] listOfTargetFiles = targetFolder.listFiles();
        for (File file : listOfQueryFiles) {
            for (File target : listOfTargetFiles) {
                int numberOfSolutions = 0;
                String graphLabel = target.getName().split("\\.")[0];
                System.out.println(file.getName());
                LinkedHashMap<String, String> nodeAndCondition = new LinkedHashMap<>();
                LinkedHashMap<String, ArrayList<String>> nodeLink = new LinkedHashMap<>();
                System.out.println(graphLabel);
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new FileReader(file));
                } catch (FileNotFoundException ex) {
                    System.err.println(ex.toString());
                }
                String line = "";
                int numberOfNodes = Integer.parseInt(reader.readLine()); // number of nodes to read
                int counter = 0;
                try {
                    while ((line = reader.readLine()) != null && counter < numberOfNodes) {
                        //System.out.println(line);
                        String node = line.split(" ")[0];
                        String condition = line.split(" ")[1];
                        nodeAndCondition.put("u" + node, "protein='" + condition + "'");
                        counter++;
                    }
                } catch (IOException ex) {
                    System.err.println(ex.toString());
                }
                int numberOfEdges = Integer.parseInt(line);
                //System.out.println(firstCount);
                counter = 0;
                try {
                    while ((line = reader.readLine()) != null) {
                        if (counter < numberOfEdges) {
                            ArrayList<String> links = new ArrayList<>();
                            String toNode = line.split(" ")[0];
                            String fromNode = line.split(" ")[1];
                            if (nodeLink.containsKey("u" + toNode)) {
                                links = nodeLink.get("u" + toNode);
                            }

                            links.add("u" + fromNode);
                            nodeLink.put("u" + toNode, links);
                            counter++;
                        } else {
                            numberOfEdges = Integer.parseInt(line);
                            counter = 0;
                        }
                    }
                    reader.close();
                } catch (IOException ex) {
                    System.err.println(ex.toString());
                }
                HashMap<String, Boolean> visitNode = new HashMap<>();
                ArrayList<String> nodes = new ArrayList<>();
                for (int i = 0; i < numberOfNodes; i++) {
                    nodes.add("u" + i);
                }
                for (String node : nodes) {
                    visitNode.put(node, false);
                }
                String match = "MATCH ";
                String lhs = "";
                ArrayList<String> rhs = new ArrayList<>();
                ArrayList<String> finalList = new ArrayList<>();
                for (HashMap.Entry<String, ArrayList<String>> entry : nodeLink.entrySet()) {
                    String temp = "";
                    String key = entry.getKey();
                    for (String value : entry.getValue()) {
                        if (visitNode.get(key) == false) {
                            lhs = "(" + key + ":" + graphLabel + ")";
                            visitNode.put(key, true);
                        } else {
                            lhs = "(" + key + ")";
                        }
                        if (visitNode.get(value) == false) {
                            temp = "(" + value + ":" + graphLabel + ")";
                            visitNode.put(value, true);
                        } else {
                            temp = "(" + value + ")";
                        }
                        rhs.add(lhs + " -[:connects]-> " + temp);

                    }
                }
                for (int i = 0; i < rhs.size() - 1; i++) {
                    match = match + rhs.get(i) + "," + "\n";
                }
                match = match + rhs.get(rhs.size() - 1) + "\n" + "WHERE" + "\n" + "   ";
                //
                for (HashMap.Entry<String, String> entry : nodeAndCondition.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    finalList.add(key + "." + value);
                }
                for (int i = 0; i < finalList.size() - 1; i++) {
                    match = match + finalList.get(i) + " AND ";
                }
                match = match + finalList.get(finalList.size() - 1) + "\n" + "RETURN ";
                for (String node : nodes) {
                    match = match + node + ",";
                }
                match = match.substring(0, match.length() - 1);
                //String outputFile = graphLabel + "_" + file.getName().split("\\.")[0]
                 //       + "_" + file.getName().split("\\.")[1] + "_output.txt";
                System.out.println(match);
                StringBuilder stringBuilder = new StringBuilder();
                try (Transaction tx = db.beginTx();
                     Result result = db.execute(match)) {
                    while (result.hasNext()) {
                        numberOfSolutions++;
                        Map row = result.next();
                        stringBuilder.append("S:").append(nodes.size()).append(":");
                        try (Transaction tx2 = db.beginTx()) {
                            for (int i = 0; i < nodes.size(); i++) {
                                Node col = (Node) row.get("u" + i);
                                if (i < nodes.size() - 1) {
                                    stringBuilder.append(i).append(",").append(col.getProperty("nodeid")).append(";");
                                } else {
                                    stringBuilder.append(i).append(",").append(col.getProperty("nodeid"));
                                }
                            }
                            tx2.success();
                        }
                        stringBuilder.append("\n");

                    }
                    System.out.println("T: "+graphLabel+".grf");
                    System.out.println("P: "+file.getName());
                    System.out.println("N: "+ numberOfSolutions);
                    System.out.println(stringBuilder);
                    tx.success();
                }
            }
        }
    }
    public static void main(String[] args) throws IOException {

        createQuery();
        }
}
