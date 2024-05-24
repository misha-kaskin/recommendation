import java.sql.Connection;
import java.sql.DriverManager;

public interface Config {
    String urlDb = "jdbc:postgresql://localhost:5432/recommend_db";
    String userName = "sa";
    String password = "password";

    static Connection initConn() {
        try {
            return DriverManager.getConnection(Config.urlDb, Config.userName, Config.password);
        } catch (Exception e) {
            return null;
        }
    }
}
