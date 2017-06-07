import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.YtDtClient;
import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.client.schema.ColumnSchema;
import ru.yandex.yt.rpc.client.schema.TableSchema;

/**
 * @author valri
 */
public class RpcExample {
    private static final String USER_ID = "user_id";
    private static final String VIEW_PAGE = "view_main_page_popular__model";
    private static final String VIEW_MODEL = "view__model";

    private static List<ColumnSchema> columns = new ArrayList<>();
    static {
        columns.add(new ColumnSchema((short) 0, "hash", ValueType.UINT_64));
        columns.add(new ColumnSchema((short) 1, USER_ID, ValueType.STRING));
        columns.add(new ColumnSchema((short) 2, VIEW_MODEL, ValueType.STRING));
        columns.add(new ColumnSchema((short) 3, VIEW_PAGE, ValueType.STRING));
    }

    public static void main(String[] args) throws Exception {
        final YtDtClient client = new YtDtClient(new InetSocketAddress("barney.yt.yandex.net", 9013),
                System.getenv("YT_TOKEN"), System.getenv("YT_LOGIN"), "yt.yandex.net", 1);
        try {
            List<Map<String, Object>> filters = new ArrayList<>();
            List<String> userIds = new ArrayList<>();
            userIds.add("yandexuid:5760449581448856830");
            for (String userId: userIds) {
                Map<String, Object> filter = new HashMap<>();
                filter.put("user_id", userId);
                filters.add(filter);
            }
            LookupReqInfo lookupInfo = new LookupReqInfo( new TableSchema(
                    "//home/market/testing/yamarec/rt-logger/ichwill_events", columns), filters, 1, UUID.randomUUID());
            System.out.println(client.lookupRows(lookupInfo, false, false));

        } finally {
            client.close();
        }
    }
}
