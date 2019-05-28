import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class SQLConnection {

    /*
        Klasse, um SQL-Anfragen an die Hauptdatenbank zu machen
     */


    private final static String database = "restaurant";
    private final static String user = "public";
    private final static String pwd = "public";
    private final static String serverAddress = "auth.xylian.org";
    private final static String sqlPort = "3306";
    private final static String sqlURL = "jdbc:mariadb://" + serverAddress + ":" + sqlPort + "/" + database + "?autoReconnect=true";

    @SuppressWarnings("WeakerAccess")
    public final static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static Connection conn;
    private static Statement stmt;


    private SQLConnection(){}


    private static void init(@SuppressWarnings("SameParameterValue") boolean skipCheck){

        if(!skipCheck && isConnected()) return;

        executorService.execute(() -> {

            try {

                Class.forName("org.mariadb.jdbc.Driver");
                conn = DriverManager.getConnection(sqlURL, user, pwd);

            } catch (Exception e){
                e.printStackTrace();
            }

        });

    }

    public static void init(){
        init(false);
    }


    public static ResultSet query(String qry){

        Future ret = executorService.submit(() -> {

            try{

                if(stmt != null && !stmt.isClosed()) stmt.close();
                stmt = conn.createStatement();
                return stmt.executeQuery(qry);

            }catch(Exception e){
                e.printStackTrace();
                return null;
            }

        });

        try {
            return (ResultSet) ret.get();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public static void update(String qry){

        executorService.execute(() -> {

            try{
                Statement stmt = conn.createStatement();
                stmt.executeUpdate(qry);
                stmt.close();
            }catch(Exception e){
                e.printStackTrace();
            }
        });
    }

    @SuppressWarnings("unused")
    public static void close(){

        executorService.execute(() -> {

            try {
                if(conn != null) conn.close();
            }catch(Exception e){
                e.printStackTrace();
            }

        });
    }

    public static void shutdown() {
        close();
        executorService.shutdownNow();
    }

    public static boolean isConnected(){

        Future ret = executorService.submit(() -> {
            try {

                if (conn == null || !conn.isValid(10))
                    return false;

            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        });

        try {
            return (boolean) ret.get();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

}
