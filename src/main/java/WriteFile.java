import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.csv.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class WriteFile {
    private static final Logger logger = LogManager.getLogger(WriteFile.class.getName());
    /**
     * @param list Records of Role assign and orphan entitlements in String Builder
     * @param pathRoleAssigned
     * @param pathOrphanEntitlements
     * @throws IOException
     */
    public void WritePath(Map<String, StringBuilder> list, String pathRoleAssigned, String pathOrphanEntitlements){
        logger.info("Starting Write");
        try {
            CSVPrinter printRA = new CSVPrinter(new FileWriter(pathRoleAssigned), CSVFormat.EXCEL);
            CSVPrinter printOE = new CSVPrinter(new FileWriter(pathOrphanEntitlements),CSVFormat.EXCEL);
            logger.debug("Adding StringBuilder to CSV records");

            printRA.printRecords(list.get("0"));
            printOE.printRecords(list.get("1"));
            logger.debug("Completed adding StringBuilder to CSV records");

            printRA.close();
            printOE.close();
            logger.info("Finished writing successfully");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("ERROR while writing " + e.toString());
        }
    }
}