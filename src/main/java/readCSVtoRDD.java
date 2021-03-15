import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class readCSVtoRDD {
    public JavaPairRDD<String, Set<String>> read(String path, JavaSparkContext sc){

        JavaRDD<String> RDD = sc.textFile(path);
        JavaPairRDD<String, Set<String>> Pair = RDD.mapToPair( inputLine -> {
            String[] col = inputLine.split(",");
            Set<String> entitlements = new HashSet<>(Arrays.asList(col).subList(1, col.length));
            System.out.println(col[0] + " has been read");
            return new Tuple2<>(col[0],entitlements);
        });
        System.out.println(path + " has been read");
        return (Pair);
    }
}
