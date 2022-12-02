import java.sql.SQLException;

public class Main {

    public static void main(String[] args) throws SQLException {

        String connectionUrl = "jdbc:sqlserver://132.72.64.124:1433;databaseName=moshayof;user=moshayof;password=7dYN61wB;encrypt=false;";
        Assignment mc=new Assignment(connectionUrl,"moshayof","7dYN61wB");
        mc.printSimilarItems(new Long(2));
    }
}