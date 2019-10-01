import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import ru.yandex.yt.jdbc.YtDriver;

public class TestJDBCQueries {

    public static void main(String[] args) throws SQLException {
        DriverManager.registerDriver(new YtDriver());

        final Properties props = new Properties();
        props.setProperty("username", "miroslav2");
        props.setProperty("token", "file:~/.yt/token");
        props.setProperty("compression", "Lz4");
        try (Connection conn = DriverManager.getConnection("jdbc:yt:/zeno", props)) {
            try (Statement st = conn.createStatement()) {
                st.setMaxRows(5);
                final String query = "select * from [//home/pricelabs/testing/shops] order by shop_id desc limit 3";
                try (ResultSet rs = st.executeQuery(query)) {
                    while (rs.next()) {
                        System.out.println(rs.getInt("shop_id") + " = " + rs.getString("name") +
                                ", " + rs.getString("schedule") + rs.getString("feed_ids"));
                    }
                }

                try (ResultSet rs = st.executeQuery(query)) {
                    while (rs.next()) {
                        System.out.println(rs.getInt("shop_id") + " += " + rs.getString("name") +
                                ", " + rs.getString("schedule") + rs.getString("feed_ids"));
                    }
                }
            }
        }
    }

}
