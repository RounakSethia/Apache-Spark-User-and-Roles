import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Manager {
    public void logic(String userPath, String rolePath, String roleAssignPath, String orphanEntitlementsPath){
        readCSVtoRDD readFile = new readCSVtoRDD();

        System.setProperty("hadoop.home.dir", "C:/Users/vibhor/Downloads/hadoop");
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Set<String>> userPair = readFile.read(userPath,sc);
        JavaPairRDD<String,Set<String>> rolePair = readFile.read(rolePath,sc);

        StringBuilder sb = new StringBuilder();

        Scanner scan = new Scanner(System.in);
        scan.nextLine();

        userPair.foreach(record -> {
            List<String> goodRoles = new ArrayList<>();
            rolePair.foreach(rec ->{
                if(record._2.containsAll(rec._2)){
                    goodRoles.add(rec._1());
                }
            });
            sb.append(stringRole(record._1,goodRoles));
        });
        System.out.println(sb);
        /*
         *
         *
        JavaPairRDD<String,Set<String>> roleAssign = userPair.mapToPair(record ->{
            Set<String> goodRoles = new HashSet<>();
            rolePair.foreach(rec ->{
                if(record._2.containsAll(rec._2)){
                    goodRoles.add(rec._1());
                }
            });
            return new Tuple2<>(record._1, goodRoles);
        });
        roleAssign.cache();
        System.out.println(roleAssign.count());
        roleAssign.foreach(System.out::println);
        */

        /**
         * Below part is only to stop from exiting while I check on localhost
         */

        String s = scan.nextLine();
    }
    private String stringRole(String userName, List<String> goodRoles){
        StringBuilder roleAssign = new StringBuilder(userName + ",");
        for (String goodRole : goodRoles) {
            roleAssign.append(goodRole).append(",");
        }
        roleAssign.append("\n");
        return(roleAssign.toString());
    }
}