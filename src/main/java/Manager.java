import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;

public class Manager {
    public void logic(String userPath, String rolePath, String roleAssignPath, String orphanEntitlementsPath){
        readCSVtoRDD readFile = new readCSVtoRDD();
        Analysis analysis = new Analysis();
        WriteFile writeFile = new WriteFile();

        System.setProperty("hadoop.home.dir", "C:/Users/vibhor/Downloads/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Set<String>> userPair = readFile.read(userPath,sc);
        JavaPairRDD<String,Set<String>> rolePair = readFile.read(rolePath,sc);
        userPair.cache();
        rolePair.cache();

        Map<String,List<String>> List = analysis.assignRole(userPair,rolePair);

        writeFile.WritePath(List,roleAssignPath,orphanEntitlementsPath);
         /**
         * Below part is only to stop from exiting while I check on localhost
         */
        Scanner scan = new Scanner(System.in);
        scan.nextLine();
    }
}