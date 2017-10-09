import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;

import java.io.*;
import java.util.*;

public class queryParser {
    public static void main(String[] args) throws IOException {
        File file = new File("C:\\Users\\vatsa\\IdeaProjects\\neo4j1\\src\\main\\java\\test.txt");
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        ArrayList<String> lines = new ArrayList<>();
        String line;
        HashMap<String, String> nodeAndLabel = new HashMap<>();
        HashMap<String, ArrayList<String>> nodeAndCondition = new HashMap<>();
        HashMap<String, ArrayList<String>> nodeLink = new HashMap<>();
        HashSet<String> nodes = new HashSet<>();
        HashSet<String> labels = new HashSet<>();
        while ((line = bufferedReader.readLine()) != null) {
            lines.add(line);
            String[] strArray = line.split(" ");
            //System.out.println(strArray[0]);
            nodes.add(strArray[0]);
            if (!nodes.contains(strArray[1])) {
                labels.add(strArray[1]);
            }
        }
        for (String str : lines) {
            ArrayList<String> links = new ArrayList<>();
            ArrayList<String> conditions = new ArrayList<>();
            String[] newArr = str.split(" ");
            if ( labels.contains(newArr[1])) {
                nodeAndLabel.put(newArr[0], newArr[1]);
            }
            if ( newArr.length > 2) {
                if (nodeAndCondition.containsKey(newArr[0])) {
                    conditions = nodeAndCondition.get(newArr[0]);
                }
                for (int i = 2; i < newArr.length; i++){
                    conditions.add(newArr[i]);
                }
                nodeAndCondition.put(newArr[0], conditions);
            }
            if (newArr.length == 2 && nodes.contains(newArr[1])) {
                if (nodeLink.containsKey(newArr[0])) {
                    links = nodeLink.get(newArr[0]);
                }
                links.add(newArr[1]);
                nodeLink.put(newArr[0], links);
            }

        }
        System.out.println(nodeAndLabel);
        fileReader.close();
        boolean[] visited = new boolean[nodes.size()];
        Arrays.fill(visited, Boolean.FALSE);
        HashMap<String, Boolean> visitNode = new HashMap<>();
        for (String node: nodes) {
            visitNode.put(node, false);
        }
        String match = "MATCH ";
        String lhs = "";
        ArrayList<String> rhs = new ArrayList<>();
        ArrayList<String> finalList = new ArrayList<>();
        for(HashMap.Entry<String, ArrayList<String>> entry : nodeLink.entrySet()) {
            String temp = "";
            String key = entry.getKey();
            for (String value : entry.getValue()) {
                if ( visitNode.get(key) == false) {
                    lhs = "(" + key + ":" + nodeAndLabel.get(key) + ")";
                    visitNode.put(key, true);
                }
                else {
                    lhs = "(" + key + ")";
                }
                if ( visitNode.get(value) == false) {
                    temp = "(" + value + ":" + nodeAndLabel.get(value) + ")";
                    visitNode.put(value, true);
                }
                else {
                    temp = "(" + value + ")";
                }
                rhs.add(lhs+"--"+temp);

            }
        }
        for (int i = 0; i < rhs.size() - 1; i++) {
            match = match + rhs.get(i) + "," + "\n";
        }
        match = match + rhs.get(rhs.size()-1) + "\n" + "WHERE" + "\n" + "   ";
        //
        for(HashMap.Entry<String, ArrayList<String>> entry : nodeAndCondition.entrySet()) {
            String key = entry.getKey();
            for (String value : entry.getValue()) {
                finalList.add(key+ "."+ value);
            }
        }
        for (int i = 0; i < finalList.size() - 1; i++) {
            match = match + finalList.get(i) + " AND ";
        }
        match = match + finalList.get(finalList.size()-1)+"\n" + "RETURN ";
        for (String node: nodes) {
            match = match + node + ",";
        }
        match = match.substring(0, match.length() - 1);
        try(  PrintWriter out = new PrintWriter( "C:\\Users\\vatsa\\IdeaProjects\\neo4j1\\src\\main\\java\\output.txt" )  ){
            out.println( match );
        }

    }


    }
