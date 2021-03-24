# Apache-Spark-User-and-Roles
Algo
    1.  Input all file path.
    2.  Read files
    3.  For every user repeat steps 4-7
    4.  For every role repeat step 5
    5.  If user entitlements contains all roles then add role to good roles
    6.  Remove redundant Roles from good roles
    7.  Add remaining entitlements in orphan entitlements
    8.  Save good roles and orphan entitlements to path
