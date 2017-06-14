echo You may have to change this first path to the location of your database.
java -Xmx500M -Xms500M -cp hsqldb.jar org.hsqldb.server.Server --database.0 file:tpch --dbname.0 tpch 

