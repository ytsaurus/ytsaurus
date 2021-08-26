package ru.yandex.yt.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.misc.io.InputStreamSourceUtils2;
import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.proxy.MappedObject;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.YtClientTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Самый простой тест
class YtConnectionTest {
    // Нужно для того, чтобы не закрывалось подключение к RPC YT (оно останется открытым, пока есть
    // хотя бы одно JDBC подключение)
    private static String url;
    private static Properties props;
    private static Connection permanentConnection;
    private static String home;
    private static String table;
    private static YtClient client;

    private static ObjectMapper mapper;
    private static ObjectWriter writer;

    private Connection connection;

    @BeforeAll
    static void initYtConnection() throws SQLException {
        DriverManager.registerDriver(new YtDriver());

        home = YtClientTest.getPath();

        props = new Properties();
        props.setProperty("username", YtClientTest.getUsername());
        props.setProperty("token", YtClientTest.getToken());
        props.setProperty("compression", "Lz4");
        props.setProperty("home", home);
        props.setProperty("scan_recursive", "true");

        url = "jdbc:yt:" + LocalYt.getAddress();
        permanentConnection = DriverManager.getConnection(url, props);

        client = ((YtConnection) permanentConnection).getClient();

        table = home + "/dir1/table1";
        YtClientTest.createDynamicTable(client, YPath.simple(table));

        final Collection<MappedObject> objects = Arrays.asList(
                new MappedObject(1, "test1"),
                new MappedObject(2, "test2"));

        YtClientTest.insertData(client, YPath.simple(table), objects,
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class));

        mapper = new ObjectMapper();
        writer = mapper.writer().withDefaultPrettyPrinter();
    }

    @AfterAll
    static void closeYtConnection() throws SQLException {
        YtClientTest.deleteDirectory(client, YPath.simple(home));
        permanentConnection.close();
    }

    @BeforeEach
    void init() throws SQLException {
        connection = DriverManager.getConnection(url, props);
    }

    @AfterEach
    void done() throws SQLException {
        connection.close();
    }

    @Test
    void databaseMetaDataGetTableTypes() throws SQLException {
        compareWith("databaseMetaDataGetTableTypes.json", connection.getMetaData().getTableTypes());
    }

    @Test
    void databaseMetaDataGetTables() throws SQLException {
        compareWith("databaseMetaDataGetTables.json", connection.getMetaData().getTables("", "", "", null));
    }

    @Test
    void databaseMetaDataGetColumns() throws SQLException {
        compareWith("databaseMetaDataGetColumns.json", connection.getMetaData().getColumns("", "", "", ""));
    }

    @Test
    void databaseMetaDataGetBestRowsIdentifiers() throws SQLException {
        compareWith("databaseMetaDataGetBestRowsIdentifiers.json",
                connection.getMetaData().getBestRowIdentifier("", "", table, 0, true));
    }

    @Test
    void databaseMetaDataGetPrimaryKeys() throws SQLException {
        compareWith("databaseMetaDataGetPrimaryKeys.json", connection.getMetaData().getPrimaryKeys("", "", table));
    }

    @Test
    void databaseMetaDataGetTypeInfo() throws SQLException {
        compareWith("databaseMetaDataGetTypeInfo.json", connection.getMetaData().getTypeInfo());
    }

    @Test
    void executeQuery() throws SQLException {
        compareWith("executeQuery.json",
                connection.createStatement().executeQuery(String.format("* from [%s]", table)));
    }

    @Test
    void executeQueryWithSelect() throws SQLException {
        compareWith("executeQuery.json",
                connection.createStatement().executeQuery(String.format("select * from [%s]", table)));
    }

    @Test
    void executeQueryWithEnding() throws SQLException {
        compareWith("executeQuery.json",
                connection.createStatement().executeQuery(String.format("* from [%s];", table)));
    }

    List<Map<String, Object>> readAll(ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final Set<String> columns = new LinkedHashSet<>(metaData.getColumnCount());
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columns.add(metaData.getColumnName(i));
        }
        final List<Map<String, Object>> rows = new ArrayList<>();
        while (resultSet.next()) {
            final Map<String, Object> map = new LinkedHashMap<>();
            for (String column : columns) {
                map.put(column, resultSet.getObject(column));
            }
            rows.add(map);
        }
        resultSet.close();
        return rows;
    }

    void compareWith(String expect, ResultSet actual) throws SQLException {
        final List<Map<String, Object>> expectRows =
                fromJson(InputStreamSourceUtils2.valueOf("classpath:" + expect).readText().trim());
        for (Map<String, Object> row : expectRows) {
            row.computeIfPresent("TABLE_NAME", (k, v) -> table);
        }
        // Люблю сравнивать многострочники - это удобно в Идее
        assertEquals(toJson(expectRows), toJson(readAll(actual)));
    }

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fromJson(String json) {
        try {
            final Map<String, Object>[] maps = mapper.readValue(json, Map[].class);
            return Arrays.asList(maps);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    String toJson(Object object) {
        try {
            return writer.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
