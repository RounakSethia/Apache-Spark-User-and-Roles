//import java.util.Scanner;

public class MainFile {
    /*
    public static void main (String[] arg){

        Scanner sc = new Scanner(System.in);
        System.out.println("Enter users and entitlements data path");
        String pathUser = sc.nextLine();
        System.out.println("Enter roles and entitlements data path");
        String pathRole = sc.nextLine();
        System.out.println("Enter path for role assign to be saved");
        String pathRoleAssigned = sc.nextLine();
        System.out.println("Enter path for orphan entitlements to be saved");
        String pathOrphanEntitlements = sc.nextLine();
        Manager manage = new Manager();
        manage.logic(pathUser,pathRole,pathRoleAssigned,pathOrphanEntitlements);
    }
    */
    public static void main (String[] arg){

        String pathUser = "C:/Users/vibhor/Downloads/Users - Sheet1.csv";
        String pathRole = "C:/Users/vibhor/Downloads/Roles - Sheet1.csv";
        String pathRoleAssigned = "C:/Users/vibhor/Downloads/RoleAssign.csv";
        String pathOrphanEntitlements = "C:/Users/vibhor/Downloads/OrphanEntitlement.csv";
        Manager manage = new Manager();
        manage.logic(pathUser,pathRole,pathRoleAssigned,pathOrphanEntitlements);
    }
}
/*
    "C:Users\vibhor\Downloads\Users - Sheet1.csv"
    "C:/Users/vibhor/Downloads/Roles - Sheet1.csv"
    "C:/Users/vibhor/Downloads/RoleAssign.csv"
    "C:/Users/vibhor/Downloads/OrphanEntitlement.csv"
*/
