import java.sql.Connection;
import java.sql.*;

public class database {
    public Connection c;
    PreparedStatement write;
    PreparedStatement read;
    PreparedStatement insert;

    public database(String siteName){
        //System.out.println(siteName);
        try {
            Class.forName("org.sqlite.JDBC");
            String dbName = "/Users/zhenhuansu/"+siteName+".db";
            c = DriverManager.getConnection("jdbc:sqlite:"+dbName);

            String dropTable = "drop table if exists test";
            Statement stmt1 = c.createStatement();
            stmt1.execute(dropTable);
            stmt1.close();

            String createTable = "create table if not exists test (item varchar(20), value int)";
            Statement stmt2 = c.createStatement();
            stmt2.execute(createTable);
            stmt2.close();

            String sqlInsert = "insert into test values (?,?)";
            this.insert = this.c.prepareStatement(sqlInsert);

            String sqlWrite = "update test set value = ? where item = ?";
            this.write = this.c.prepareStatement(sqlWrite);

            String sqlRead = "select value from test where item = ?";
            this.read = this.c.prepareStatement(sqlRead);

        } catch (ClassNotFoundException e) {
            System.out.println("connect driver fail");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("connect database fail");
            e.printStackTrace();
        }
        System.out.println("Opened database successfully");

    }

    public synchronized int write(String item, int value) throws SQLException{
        write.setInt(1,value);
        write.setString(2,item);
        write.executeUpdate();
        return value;
    }

    public synchronized int read(String item) throws SQLException{
        read.setString(1,item);
        ResultSet rs = read.executeQuery();
        int value = rs.getInt("value");
        rs.close();
        return value;
    }

    public synchronized void inset(String item, int value) throws SQLException{
        insert.setString(1,item);
        insert.setInt(2,value);
        insert.executeUpdate();
    }

//    public void insert(String letter, int i) {
//    }
}
