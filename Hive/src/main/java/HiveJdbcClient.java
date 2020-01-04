import java.sql.*;

/**
 * @author: YangFei
 * @description: jbeeline客户端使用
 * @create:2020-01-04 14:47
 */
public class HiveJdbcClient {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection("jdbc:hive2://server1:10000/default", "hive", "hive");
        Statement stmt = conn.createStatement();
        String sql = "select * from people3";
        ResultSet resultSet = stmt.executeQuery(sql);
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "-" + resultSet.getString("name"));
        }
    }
}
