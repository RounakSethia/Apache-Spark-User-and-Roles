import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Scanner;
import java.util.Set;

public class Manager {
    public void logic(String userPath, String rolePath, String roleAssignPath, String orphanEntitlementsPath){
        readCSVtoRDD readFile = new readCSVtoRDD();

        System.setProperty("hadoop.home.dir", "C:/Users/vibhor/Downloads/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[3]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Set<String>> userPair = readFile.read(userPath,sc);
        JavaPairRDD<String,Set<String>> rolePair = readFile.read(rolePath,sc);

        StringBuilder sb = new StringBuilder();
        userPair.cache();
        rolePair.cache();
        //System.out.println(rolePair.countByValue() + "\n");

        /**
        List<String> goodRoles = new ArrayList<>();
        userPair.foreach(record -> {
            rolePair.foreach(rec ->{
                if(record._2.containsAll(rec._2)){
                    goodRoles.add(rec._1());
                }
            });
            sb.append(stringRole(record._1,goodRoles));
            goodRoles.clear();
        });
        System.out.println(sb);
         */

        /**
         * Below part is only to stop from exiting while I check on localhost
         */
        Scanner scan = new Scanner(System.in);
        scan.nextLine();
        System.out.println(rolePair.values().collect());


        /**
        JavaRDD<Set<String>> setsOfRoles = rolePair.values();
        userPair.foreach(record -> {
            System.out.println(setsOfRoles.filter(record._2::containsAll).collect());
        });
        */

        /**
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
    }
}