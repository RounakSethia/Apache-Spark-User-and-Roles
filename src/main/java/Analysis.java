import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.*;

public class Analysis {
    public Map<String,List<String>> assignRole(JavaPairRDD<String, Set<String>> userPair,JavaPairRDD<String, Set<String>> rolePair){
        List<String> listRole = new ArrayList<>();
        List<String> listOrphan = new ArrayList<>();

        for (Tuple2<String, Set<String>> user : userPair.collect()){
            JavaPairRDD<String, Set<String>> goodRoles = rolePair.filter(line -> user._2.containsAll(line._2)).repartition(5);
            goodRoles.cache();
            listRole.add(user._1 + removeRedundant(goodRoles));
            listOrphan.add(orphanEntitlements(user,goodRoles));
        }
        Map<String, List<String>> map = new HashMap<>();
        map.put("0",listRole);
        map.put("1",listOrphan);
        return (map);
    }
    private String removeRedundant(JavaPairRDD<String, Set<String>> goodRoles){
        String roles = "";
        for (Tuple2<String,Set<String>> removeRedundant : goodRoles.collect()){
            if(goodRoles.filter(line-> line._2.containsAll(removeRedundant._2)).count()==1){
                roles += "," + removeRedundant._1;
            }
        }
        roles += "\n";
        return roles;
    }
    private String orphanEntitlements(Tuple2<String, Set<String>> user, JavaPairRDD<String, Set<String>> goodRoles){
        for(Tuple2<String,Set<String>> orphan : goodRoles.collect()){
            user._2.removeAll(orphan._2);
        }
        return user._1 + "," + String.join(",", user._2) + "\n";
    }
}
