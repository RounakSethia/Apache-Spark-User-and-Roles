import org.apache.spark.api.java.JavaPairRDD;

import java.util.*;

public class Analysis {
    public static List<String> listRoleAssign = new ArrayList<>();
    public static List<String> listOrphanEntitlements = new ArrayList<>();
    private static int i=0;

    public static Map<String,List<String>> assignRole(JavaPairRDD<String, Set<String>> userPair, JavaPairRDD<String, Set<String>> rolePair){
        userPair = userPair.cache();
        Map<String,Set<String>> roleMap = rolePair.collectAsMap();

        userPair.foreach(userList -> {
            /*List<String> goodRoles = new ArrayList<>();
            for (Map.Entry<String,Set<String>> roleEntry : roleMap.entrySet()) {
                Set<String> roleEntitlements = new HashSet<>(roleMap.get(roleEntry.getKey()));
                if (userList._2.containsAll(roleEntitlements))
                    goodRoles.add(roleEntry.getKey());
            }
            removeRedundant(goodRoles, roleMap);
            stringRole(userList._1,goodRoles);
            orphanEntitlements(goodRoles,roleMap,userList._2, userList._1);*/
            //System.out.print(userList._1);
            i++;
        });
        System.out.println("\n" + i + "        " + userPair.count());
        System.out.println(listRoleAssign.size() +"\t\t\t\t\t" + listOrphanEntitlements.size());
        /** This is for when you want every role to be partitioned and executed according to it.
         *
         * for (Tuple2<String, Set<String>> user : userPair.collect()){
            JavaPairRDD<String, Set<String>> goodRoles = rolePair.filter(line -> user._2.containsAll(line._2));
            goodRoles = goodRoles.cache();
            System.out.print(user._1 + removeRedundant2(goodRoles));
            listRoleAssign.add(user._1 + removeRedundant2(goodRoles));
            listOrphanEntitlements.add(orphanEntitlements2(user,goodRoles));
        }*/

        Map<String, List<String>> map = new HashMap<>();
        map.put("0",listRoleAssign);
        map.put("1",listOrphanEntitlements);
        return (map);
    }

    public static List<String> removeRedundant(List<String> goodRoles, Map<String, Set<String>> roleMap) {
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
    public static void orphanEntitlements(List<String> goodRoles, Map<String, Set<String>> roleMap, Set<String> userEntitlements, String userName) {
        for (String goodRole : goodRoles) {
            userEntitlements.removeAll(roleMap.get(goodRole));
        }
        listOrphanEntitlements.add(userName + "," + String.join(",", userEntitlements) + "\n");
    }
    private static void stringRole(String userName, List<String> goodRoles){
        StringBuilder roleAssign = new StringBuilder(userName + ",");
        for (String goodRole : goodRoles) {
            roleAssign.append(goodRole).append(",");
        }
        roleAssign.append("\n");
        listRoleAssign.add(roleAssign.toString());
    }
    /**private static String orphanEntitlements2(Tuple2<String, Set<String>> user, JavaPairRDD<String, Set<String>> goodRoles){
        for(Tuple2<String,Set<String>> orphan : goodRoles.collect()){
            user._2.removeAll(orphan._2);
        }
        return user._1 + "," + String.join(",", user._2) + "\n";
    }
    private static String removeRedundant2(JavaPairRDD<String, Set<String>> goodRoles){
        String roles = "";
        for (Tuple2<String,Set<String>> removeRedundant : goodRoles.collect()){
            if(goodRoles.filter(line-> line._2.containsAll(removeRedundant._2)).count()==1){
                roles += "," + removeRedundant._1;
            }
        }
        roles += "\n";
        return roles;
    }*/
}