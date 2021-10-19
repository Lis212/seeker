import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ForkJoinPool;

public class Main {
    public static void main(String[] args) {
        Connection connection = DBConnection.getConnection();
        LinkParser linkParser = new LinkParser("https://skillbox.ru/");
        new ForkJoinPool().invoke(linkParser);
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
