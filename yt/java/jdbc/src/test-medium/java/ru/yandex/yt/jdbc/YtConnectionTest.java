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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.misc.io.InputStreamSourceUtils2;
import ru.yandex.yt.client.proxy.MappedObject;
import ru.yandex.yt.client.proxy.YtClientTest;
import ru.yandex.yt.ytclient.proxy.YtClient;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Самый простой тест
class YtConnectionTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(YtConnectionTest.class);

    // Нужно для того, чтобы не закрывалось подключение к RPC YT (оно останется открытым, пока есть
    // хотя бы одно JDBC подключение)
    private static String URL;
    private static Properties PROPS;
    private static Connection PERMANENT_CONNECTION;
    private static String HOME;
    private static String TABLE;
    private static YtClient CLIENT;

    private static ObjectMapper MAPPER;
    private static ObjectWriter WRITER;

    @BeforeAll
    static void initYtConnection() throws SQLException {
        DriverManager.registerDriver(new YtDriver());

        HOME = YtClientTest.getPath();

        final Properties props = new Properties();
        props.setProperty("username", YtClientTest.getUsername());
        props.setProperty("token", YtClientTest.getToken());
        props.setProperty("compression", "Lz4");
        props.setProperty("home", HOME);
        props.setProperty("scan_recursive", "true");

        URL = "jdbc:yt:" + YtClientTest.getProxy();
        PROPS = props;
        PERMANENT_CONNECTION = DriverManager.getConnection(URL, PROPS);

        CLIENT = ((YtConnection) PERMANENT_CONNECTION).getClient();

        final String dir = HOME + "/dir1";
        TABLE = dir + "/table1";
        YtClientTest.createDirectory(CLIENT, dir);
        YtClientTest.createTable(CLIENT, TABLE);

        final Collection<MappedObject> objects = Arrays.asList(
                new MappedObject(1, "test1"),
                new MappedObject(2, "test2"));

        YtClientTest.insertData(CLIENT, TABLE, objects,
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class));

        MAPPER = new ObjectMapper();
        WRITER = MAPPER.writer().withDefaultPrettyPrinter();
    }

    @AfterAll
    static void closeYtConnection() throws SQLException, InterruptedException {
        YtClientTest.deleteDirectory(CLIENT, HOME);
        PERMANENT_CONNECTION.close();
    }

    private Connection connection;
    private String table;

    private YTreeObjectSerializer<MappedObject> serializer;

    @BeforeEach
    void init() throws SQLException {
        connection = DriverManager.getConnection(URL, PROPS);
        table = TABLE;
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
            final Map<String, Object>[] maps = MAPPER.readValue(json, Map[].class);
            return Arrays.asList(maps);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    String toJson(Object object) {
        try {
            return WRITER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}