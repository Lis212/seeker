import java.sql.*;

public class DBConnection {
    private static Connection connection;

    private static String dbName = "search_engine";
    private static String dbUser = "search_admin";
    private static String dbPass = "search_test";

    public static Connection getConnection() {
        if (connection == null) {
            try {
                connection = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:5432/" + dbName +
                                "?user=" + dbUser + "&password=" + dbPass);
                connection.createStatement().execute("DROP TABLE IF EXISTS index");
                connection.createStatement().execute("DROP TABLE IF EXISTS page");
                connection.createStatement().execute("DROP TABLE IF EXISTS field");
                connection.createStatement().execute("DROP TABLE IF EXISTS lemma");

                connection.createStatement().execute("CREATE TABLE IF NOT EXISTS page(" +
                        "id BIGSERIAL PRIMARY KEY NOT NULL, " +
                        "path TEXT NOT NULL, " +
                        "code INT NOT NULL, " +
                        "content TEXT NOT NULL)");
                connection.createStatement().execute("CREATE UNIQUE INDEX IF NOT EXISTS path_index ON page(path)");

                connection.createStatement().execute("CREATE TABLE IF NOT EXISTS field(" +
                        "name VARCHAR(255) NOT NULL, " +
                        "selector VARCHAR(255) NOT NULL, " +
                        "weight FLOAT NOT NULL)");
                connection.createStatement().execute("INSERT INTO field(name, selector, weight) " +
                        "VALUES ('title', 'title', 1.0), ('body', 'body', 0.8)");

                connection.createStatement().execute("CREATE TABLE IF NOT EXISTS lemma(" +
                        "id BIGSERIAL PRIMARY KEY NOT NULL, " +
                        "lemma VARCHAR(255) NOT NULL, " +
                        "frequency INT NOT NULL, " +
                        "CONSTRAINT lemma_key UNIQUE (lemma))");

                connection.createStatement().execute("CREATE TABLE IF NOT EXISTS index(" +
                        "id BIGSERIAL PRIMARY KEY NOT NULL, " +
                        "page_id INT NOT NULL, " +
                        "lemma_id INT NOT NULL," +
                        "rank FLOAT NOT NULL, " +
                        "FOREIGN KEY (page_id) REFERENCES page (id), " +
                        "FOREIGN KEY (lemma_id) REFERENCES lemma (id), " +
                        "CONSTRAINT page_lemma_key UNIQUE (page_id, lemma_id))");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static int insertToPage(String path, int code, String content) {
        String sql = "INSERT INTO page(path, code, content) " +
                "VALUES (?, ?, ?) RETURNING id";
        try (PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, path);
            ps.setInt(2, code);
            ps.setString(3, content);
            ps.execute();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt("id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static int insertOrUpdateToLemma(String lemma, int frequency) {
        String sql = "INSERT INTO lemma(lemma, frequency) " +
                "VALUES (?, ?) ON CONFLICT (lemma) DO UPDATE SET frequency = lemma.frequency + 1 " +
                "RETURNING id";
        try (PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, lemma);
            ps.setInt(2, frequency);
            ps.execute();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            if (generatedKeys.next()) {
                int anInt = generatedKeys.getInt("id");
                return anInt;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static int insertOrUpdateToIndex(int pageId, int lemmaId, float rank) {
        String sql = "INSERT INTO index(page_id, lemma_id, rank) " +
                "VALUES (?, ?, ?) ON CONFLICT (page_id, lemma_id) DO UPDATE SET rank = index.rank + EXCLUDED.rank " +
                "RETURNING id";
        try (PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, pageId);
            ps.setInt(2, lemmaId);
            ps.setFloat(3, rank);
            ps.execute();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt("id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void multiInsertOrUpdateToIndex(String bigSql) {
        String sql = "INSERT INTO index(page_id, lemma_id, rank) " +
                "VALUES " + bigSql + " ON CONFLICT (page_id, lemma_id) DO UPDATE SET rank = index.rank + EXCLUDED.rank " +
                "RETURNING id";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
