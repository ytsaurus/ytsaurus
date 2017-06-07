import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.YtDtAsyncClient;
import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.client.schema.ColumnSchema;
import ru.yandex.yt.rpc.client.schema.TableSchema;

/**
 * @author valri
 */
public class AsyncRpcExample {
    public static void main(String[] args) throws Exception {
        final YtDtAsyncClient client = new YtDtAsyncClient(new InetSocketAddress("barney.yt.yandex.net", 9013),
                System.getenv("YT_TOKEN"), System.getenv("YT_LOGIN"), "yt.yandex.net", 1);
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema((short)0 , "PageID" , ValueType.INT_64));
        columns.add(new ColumnSchema((short)1 , "Name" , ValueType.STRING, false));
        columns.add(new ColumnSchema((short)2 , "Description" , ValueType.STRING, false));
        columns.add(new ColumnSchema((short)3 , "TargetType" , ValueType.INT_64, false));
        columns.add(new ColumnSchema((short)4 , "CreateTime" , ValueType.INT_64));
        columns.add(new ColumnSchema((short)5 , "StartTime" , ValueType.INT_64, false));
        columns.add(new ColumnSchema((short)6 , "PageSelect" , ValueType.INT_64));
        TableSchema schema = new TableSchema("//yabs/Dicts/Page2", columns);
        List<Map<String, Object>> filter = new ArrayList<>();
        Map<String, Object> firstFilter = new HashMap<>();
        firstFilter.put("PageID", 25);
        Map<String, Object> secondFilter = new HashMap<>();
        secondFilter.put("PageID", 12);
        filter.add(firstFilter);
        filter.add(secondFilter);
        LookupReqInfo lookupInfo = new LookupReqInfo(schema, filter, 1, UUID.randomUUID());

        CompletableFuture<List<Map<String, Object>>> val = client.lookupRows(lookupInfo, false, false);
        val.whenCompleteAsync((result, throwable) -> {
            if (throwable == null) {
                System.out.println(result);
            } else {
                System.out.println("Failed to get request");
            }
            try {
                client.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
