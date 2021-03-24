import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Map;

public class WriteFile {
    private static final Logger logger = LogManager.getLogger(WriteFile.class.getName());
    /**
     * @param list Records of Role assign and orphan entitlements
     * @param pathRoleAssigned Path
     * @param pathOrphanEntitlements Path
     */
    public void WritePath(Map<String, List<String>> list, String pathRoleAssigned, String pathOrphanEntitlements){
        logger.info("Starting Write");
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[3]").set("spark.executor.memory","2g");
        JavaSparkContext sc = new JavaSparkContext(conf);

        logger.debug("Adding list to RDD");
        JavaRDD<String> printRA = sc.parallelize(list.get("0"),4);
        JavaRDD<String> printOE = sc.parallelize(list.get("1"),4);
        logger.debug("Completed \n Saving RDD to Path");

        printRA.saveAsTextFile(pathRoleAssigned);
        printOE.saveAsTextFile(pathOrphanEntitlements);
        logger.info("Finished writing successfully");
    }
}
