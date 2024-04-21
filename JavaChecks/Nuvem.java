import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Nuvem {
    
    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    Connection con = DriverManager.getConnection("jdbc:mysql://localhost/t", "", "");
    
    Statement st = con.createStatement();
    String sql = ("SELECT * FROM posts ORDER BY id DESC LIMIT 1;");
    ResultSet rs = st.executeQuery(sql);
    if(rs.next()) { 
     int id = rs.getInt("first_column_name"); 
     String str1 = rs.getString("second_column_name");
    }
    
    con.close();
    }

}
