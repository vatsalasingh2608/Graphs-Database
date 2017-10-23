import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

public class naiveSubgraphMatching {
    //static GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File("C:/Protein") );
    static GraphDatabaseService db = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(new File("C:/Protein"))
            .setConfig(GraphDatabaseSettings.pagecache_memory, "512M" )
            .setConfig(GraphDatabaseSettings.string_block_size, "60" )
            .setConfig(GraphDatabaseSettings.array_block_size, "300" )
            .newGraphDatabase();
    public static ArrayList<String> findLabels() {
        ArrayList<String> labels = new ArrayList<>();
        String labelQuery = "MATCH (n) RETURN distinct labels(n)";
        try (Transaction ignored = db.beginTx();
             Result result = db.execute( labelQuery )) {
            while (result.hasNext()) {
                Map<String,Object> rows = result.next();
                for (Map.Entry row: rows.entrySet()) {
                    String temp = row.getValue().toString();
                    String tempArr = temp.split("\\[")[1].split("\\]")[0];
                    labels.add(tempArr);
                }
            }
        }
        return labels;
    }
    public static ArrayList<String> computeSearchSpace(String label, String attribute, int numOfNeighbors) {
        ArrayList<String> res = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> iter = db.findNodes(Label.label(label), "protein", attribute);
            while (iter.hasNext()) {
                Node node1 = iter.next();
                res.add("v"+node1.getProperty("nodeid").toString());
            }
            tx.success();
        }

        return res;
    }

    public static void match() throws IOException {
        ArrayList<String> labels = findLabels();
        String queryPath = "C:\\Users\\vatsa\\Documents\\Semester 4\\Graphs" +
                "\\Assignment 4\\Proteins\\Proteins\\part3_Proteins\\Proteins\\query";
        File queryFolder = new File(queryPath);
        File[] listOfQueryFiles = queryFolder.listFiles();
        for (File queryFile: listOfQueryFiles) {
            System.out.println(queryFile.getName());
            LinkedHashMap<String, String> nodeAndCondition = new LinkedHashMap<>();
            LinkedHashMap<String, ArrayList<String>> nodeLink = new LinkedHashMap<>();
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(queryFile));
            } catch (FileNotFoundException ex) {
                System.err.println(ex.toString());
            }
            String line = "";
            int numberOfNodes = Integer.parseInt(reader.readLine()); // number of nodes to read
            ArrayList<String> nodes = new ArrayList<>();
            for (int i = 0; i < numberOfNodes; i++) {
                nodes.add("u" + i);
            }
            int counter = 0;
            try {
                while ((line = reader.readLine()) != null && counter < numberOfNodes) {
                    String node = line.split(" ")[0];
                    String condition = line.split(" ")[1];
                    nodeAndCondition.put("u" + node, condition );
                    counter++;
                }
            } catch (IOException ex) {
                System.err.println(ex.toString());
            }
            int numberOfEdges = Integer.parseInt(line);
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
            // calculating matching order
            ArrayList<String> matchOrder = new ArrayList<>();
            LinkedHashMap<String, Boolean> visited = new LinkedHashMap<>();
            for (String node: nodes) {
                visited.put(node, false);
            }
            Stack<String> stack = new Stack<>();
            String firstNode = nodes.get(0);
            stack.add(firstNode);
            visited.put(firstNode, true);
            while (!stack.isEmpty())
            {
                String element = stack.pop();
                matchOrder.add(element);
                ArrayList<String> neighbours = nodeLink.get(element);
                for (int i = 0; i < neighbours.size(); i++) {
                    String n = neighbours.get(i);
                    if(n != null && !visited.get(n))
                    {
                        stack.add(n);
                        visited.put(n, true);

                    }
                }
            }
            System.out.println("Done!");
            for (String label: labels) {
                System.out.println(label);
                LinkedHashMap<String, ArrayList<String>> queryNodeSearchSpace = new LinkedHashMap<>();
                // compute search space
                for (String node : nodes) {
                    String nodeAttribute = nodeAndCondition.get(node);
                    int numOfNeighbors = nodeLink.get(node).size();
                    ArrayList<String> searchSpace = computeSearchSpace(label, nodeAttribute, numOfNeighbors);
                    queryNodeSearchSpace.put(node, searchSpace);
                }
                // backtracking starts here
                LinkedHashMap<String, String> solution = new LinkedHashMap<>();
                int i = 0;
                int NumberOfsolutions = subgraphMatching(solution, queryNodeSearchSpace,matchOrder, nodes, nodeAndCondition,
                        nodeLink, label, i);
                System.out.println("T: "+ label + ".grf");
                System.out.println("P: "+ queryFile.getName());
                System.out.println("N: "+NumberOfsolutions);
            }
        }
            // subgraph matching

        }
        public static int subgraphMatching(LinkedHashMap<String, String> solution, LinkedHashMap<String, ArrayList<String>> queryNodeSearchSpace,
                                                                     ArrayList<String> matchOrder, ArrayList<String> nodes,LinkedHashMap<String, String> nodeAndCondition,
                                                                     LinkedHashMap<String, ArrayList<String>> nodeLink, String label, int i){
        int numberOfSolutions = 0;
        if (solution.size() == nodes.size()) {
            System.out.println(solution);
            numberOfSolutions++;
        }
        else {
            System.out.println("Inside subgraph else loop");
            String u = matchOrder.get(i);
            ArrayList<String> tempSearchSpace = new ArrayList<>();
            for (String node : queryNodeSearchSpace.get(u)){
                if ( !solution.containsValue(node)) {
                    tempSearchSpace.add(node);
                }
            }
            for (String v: tempSearchSpace){
                solution.put(u, v);
                if ( checking(u, v, solution, label, nodes, nodeAndCondition, nodeLink)){
                    subgraphMatching(solution, queryNodeSearchSpace, matchOrder, nodes, nodeAndCondition, nodeLink, label, i+1);
                }
                solution.remove(u,v);
            }
        }
        return numberOfSolutions;
        }
        public static boolean checking(String u, String v, LinkedHashMap<String, String> solution, String label, ArrayList<String> nodes,
                                       LinkedHashMap<String, String> nodeAndCondition, LinkedHashMap<String, ArrayList<String>> nodeLink) {
            boolean ret = true;
            ArrayList<String> neighborsOfuInQueryGraph = nodeLink.get(u);
            for (String m: neighborsOfuInQueryGraph) {
                if (ret == true) {
                    if ( solution.containsKey(m)){
                        //String sOfM = solution.get(m);
                        //ArrayList<String> neighborsOfvInG = executeQueryAndReturnNeighbor(label, u);

                        if ( checkForEdge(u, v, label)) {
                            ret = true;
                        }
                        else {
                            return false;
                        }
                    }
                }
            }
            return ret;
        }

        public static boolean checkForEdge(String u, String v, String label) {
            ArrayList<String> neighbors = new ArrayList<>();
            String query = "MATCH (n:" + label
                    + "{nodeid: "+ v.charAt(1) + "})-[:connects]->(p:" + label
                    + "{nodeid: " + u.charAt(1) + "}) RETURN DISTINCT p";
            Result result = db.execute(query);
        // System.out.println(query);
            while (result.hasNext()) {
                Map row = result.next();
                try (Transaction tx = db.beginTx()) {
                    Node rowCol = (Node) row.get("p");
                    neighbors.add("v" + rowCol.getProperty("nodeid"));
                    tx.success();
                }
            }
            return (neighbors.size() > 0);
        }


    public static void main(String[] args) throws IOException {
        //findLabels();
        match();
        //computeSearchSpace("backbones_140L", "N", );
    }


}
