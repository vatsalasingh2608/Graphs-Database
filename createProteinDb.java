import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.*;

public class createProteinDb {
    public static void deleteFileOrDirectory( final File file ) {
        if ( file.exists() ) {
            if ( file.isDirectory() ) {
                for ( File child : file.listFiles() ) {
                    deleteFileOrDirectory( child );
                }
            }
            file.delete();
        }
    }
    public static void main(String[] args) throws IOException {
        String path = "C:\\Users\\vatsa\\Documents\\Semester 4\\Graphs" +
                "\\Assignment 4\\Proteins\\Proteins\\part3_Proteins\\Proteins\\target";
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        File storeDir = new File("C:/Protein");
        deleteFileOrDirectory(storeDir);
        GraphDatabaseService graphDb= new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        for (File file: listOfFiles) {
            System.out.println(file.getName());
            String[] temp = file.getName().split(".grf");
            String graphLabel = temp[0];
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException ex) {
                System.err.println(ex.toString());
            }
            String line = "";
            int N = Integer.parseInt(reader.readLine()); // number of nodes to read
            int counter = 0;
            try {
                while ((line = reader.readLine()) != null && counter < N) {
                    try (Transaction tx = graphDb.beginTx()) {
                        String[] lineSplit = line.split(" ");
                        int nodeid = Integer.parseInt(lineSplit[0]);
                        String charLabel = lineSplit[1];
                        Node node = graphDb.createNode(Label.label(graphLabel));
                        node.setProperty("protein", charLabel);
                        node.setProperty("nodeid", nodeid);
                        //System.out.println(nodeid);
                        tx.success();
                    }
                    counter++;
                }
            } catch (IOException ex) {
                System.err.println(ex.toString());
            }
            int firstCount = Integer.parseInt(line);
            //System.out.println(firstCount);
            counter = 0;
            try {
                while ((line = reader.readLine()) != null) {
                    if (counter < firstCount) {
                        try (Transaction tx = graphDb.beginTx()) {
                            Node node1 = graphDb.findNode(Label.label(temp[0]), "nodeid", Integer.parseInt(line.split(" ")[0]));
                            //System.out.println("found node 1");
                            Node node2 = graphDb.findNode(Label.label(temp[0]), "nodeid", Integer.parseInt(line.split(" ")[1]));
                            //System.out.println("found node 2");
                            node1.createRelationshipTo(node2, RelationshipType.withName("connects"));
                            //System.out.println("made the edge");
                            tx.success();
                        }
                        counter++;
                    }
                    else {
                        firstCount = Integer.parseInt(line);
                        //System.out.println(firstCount);
                        counter = 0;
                    }
                }
                reader.close();
            }
            catch (IOException ex) {
                System.err.println(ex.toString());
            }
            }
        }
    }
