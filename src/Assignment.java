import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Assignment {
    Connection con;
    Assignment(String conString,String username,String password){
        try{
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            this.con=DriverManager.getConnection(conString,username,password);
        }
        catch (ClassNotFoundException e) {
            System.out.println(e);
        } catch (SQLException e) {
            System.out.println(e);
        }

    }
    public void fileToDataBase(String path) throws SQLException {
        List<List<String>> records =this.readCsv(path);
        for (List<String> rec:
             records) {
            String query =  "insert into MediaItems (TITLE, PROD_YEAR, TITLE_LENGTH) values (?,?,?)";
                PreparedStatement ps=con.prepareStatement(query);
                ps.setString(1,rec.get(0));
                ps.setString(2,rec.get(1));
                ps.setInt(3,rec.get(0).length());
                ps.executeUpdate();
                ps.close();
        }
        con.commit();
    }
    public void calculateSimilarity() throws SQLException {
            //getting max distance
            CallableStatement cs=con.prepareCall("{?=call MAXIMALDISTANCE()}");
            cs.registerOutParameter(1,Types.FLOAT);
            cs.execute();
           float maxDistance=cs.getFloat(1);
            //getting all media ids
            PreparedStatement ps=con.prepareStatement(
                   "select med1.mid,med2.mid from MediaItems as med1,MediaItems as med2"
            );

            ResultSet rs=ps.executeQuery();

            ArrayList<int[]> check_list = new ArrayList<>();
            while (rs.next()){
                int med1=rs.getInt(1);
                int med2=rs.getInt(2);
                int[] arr = {med1,med2};
                boolean flag = false;
                for (int i = 0; i < check_list.size(); i++) {
                    if (check_list.get(i)[0]== med2 && check_list.get(i)[1]== med1){
                        flag=true;
                        break;
                    }

                }

                if (!flag){
                    CallableStatement csSim = con.prepareCall("{? = call SIMCALCULATION(?,?,?)}");
                    csSim.registerOutParameter(1, Types.FLOAT);
                    csSim.setInt(2, med1);
                    csSim.setInt(3, med2);
                    csSim.setFloat(4, maxDistance);
                    csSim.execute();

                    float sim = csSim.getFloat(1);

                    PreparedStatement psSim = con.prepareStatement("insert into Similarity values(?,?,?)");
                    psSim.setInt(1, med1);
                    psSim.setInt(2, med2);
                    psSim.setFloat(3, sim);
                    psSim.executeUpdate();
                    psSim.close();

                    check_list.add(arr);

                }
            }
            ps.close();
    }
    public void printSimilarItems(Long mid) throws SQLException {
        String query = """
                  select mi1.TITLE,mi2.TITLE,sim.SIMILARITY
                  from SIMILARITY as sim inner join MediaItems as mi1 on sim.MID1=mi1.MID inner join MediaItems as mi2 on sim.MID2=mi2.MID
                  where SIMILARITY>=0.3 and sim.mid1=?
                """;
        PreparedStatement ps=con.prepareStatement(query);
        ps.setLong(1,mid);
        ResultSet rs=ps.executeQuery();
        while (rs.next()){
            ArrayList<String> arr=new ArrayList<String>();
            arr.add(rs.getString(1));
            arr.add(rs.getString(2));
            arr.add(rs.getString(3));
            System.out.println(arr.toString());
        }
    }
    private List<List<String>> readCsv(String path){
        List<List<String>> records = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return records;
    }
}
