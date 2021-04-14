import org.apache.spark.api.java.JavaPairRDD;
import java.util.*;

public class Analysis {
    private static List<String> listRoleAssign = new ArrayList<>();
    private static List<String> listOrphanEntitlements = new ArrayList<>();

    /**
     * Analysis compare user entitlements with role entitlements and assigns then roles and unassigned entitlements as OrphanEntitlements
     * @param userPair list of user name and entitlements
     * @param rolePair list of role name and entitlements
     * @return List of assigned role and unassigned entitlements as OE in form of map
     */
    public static Map<String,List<String>> assignRole(JavaPairRDD<String, Set<String>> userPair, JavaPairRDD<String, Set<String>> rolePair){
        userPair = userPair.cache();
        Map<String,Set<String>> roleMap = rolePair.collectAsMap();

        userPair.foreach(userList -> {
            List<String> goodRoles = new ArrayList<>();
            for (Map.Entry<String,Set<String>> roleEntry : roleMap.entrySet()) {
                Set<String> roleEntitlements = new HashSet<>(roleMap.get(roleEntry.getKey()));
                if (userList._2.containsAll(roleEntitlements))
                    goodRoles.add(roleEntry.getKey());
            }
            removeRedundant(goodRoles, roleMap);
            listRoleAssign.add(stringRole(userList._1,goodRoles));
            listOrphanEntitlements.add(orphanEntitlements(goodRoles,roleMap,userList._2, userList._1));
        });

        Map<String, List<String>> map = new HashMap<>();
        map.put("0",listRoleAssign);
        map.put("1",listOrphanEntitlements);
        return (map);
    }

    private static List<String> removeRedundant(List<String> goodRoles, Map<String, Set<String>> roleMap) {
        for (int i = 0; i < (goodRoles.size() - 1); i++) {
            for (int j = i + 1; j < goodRoles.size(); j++) {
                if (roleMap.get(goodRoles.get(i)).containsAll(roleMap.get(goodRoles.get(j)))) {
                    goodRoles.remove(j);
                    j--;
                    continue;
                }
                if (roleMap.get(goodRoles.get(j)).containsAll(roleMap.get(goodRoles.get(i)))) {
                    goodRoles.remove(i);
                    i--;
                    break;
                }
            }
        }
        return goodRoles;
    }

    private static String orphanEntitlements(List<String> goodRoles, Map<String, Set<String>> roleMap, Set<String> userEntitlements, String userName) {
        for (String goodRole : goodRoles) {
            userEntitlements.removeAll(roleMap.get(goodRole));
        }
        return (userName + "," + String.join(",", userEntitlements) + "\n");
    }

    private static String stringRole(String userName, List<String> goodRoles){
        StringBuilder roleAssign = new StringBuilder(userName + ",");
        for (String goodRole : goodRoles) {
            roleAssign.append(goodRole).append(",");
        }
        roleAssign.append("\n");
        return (roleAssign.toString());
    }
}