import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;

public class Manager {
    public void logic(String userPath, String rolePath, String roleAssignPath, String orphanEntitlementsPath) {
        readCSVtoRDD readFile = new readCSVtoRDD();
        Analysis analysis = new Analysis();
        WriteFile writeFile = new WriteFile();

        System.setProperty("hadoop.home.dir", "C:/Users/vibhor/Downloads/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        //conf.set("spark.serializer","org.apache.spark.serializer.kryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Set<String>> userPair = readFile.read(userPath,sc);
        JavaPairRDD<String,Set<String>> rolePair = readFile.read(rolePath,sc);

        Map<String, List<String>> listMap = analysis.assignRole(userPair,rolePair);

        JavaRDD<String> RDDRoleAssign = sc.parallelize(listMap.get("0"));
        //System.out.println(RDDRoleAssign.count() + "        " + listMap.get("0").size());
        writeFile.WritePath(listMap,roleAssignPath,orphanEntitlementsPath);
        /**
         * Below part is only to stop from exiting while I check on localhost
         *
         Scanner scan = new Scanner(System.in);
         scan.nextLine();
        */
    }
}