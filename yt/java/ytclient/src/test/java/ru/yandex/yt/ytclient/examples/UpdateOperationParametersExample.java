package ru.yandex.yt.ytclient.examples;

import java.util.Map;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.request.UpdateOperationParameters;
import ru.yandex.yt.ytclient.proxy.request.UpdateOperationParameters.ResourceLimits;
import ru.yandex.yt.ytclient.proxy.request.UpdateOperationParameters.SchedulingOptions;

import static ru.yandex.yt.ytclient.examples.ExamplesUtil.createConnector;
import static ru.yandex.yt.ytclient.examples.ExamplesUtil.getCredentials;

public class UpdateOperationParametersExample {
    public static void main(String[] args) {
        GUID operation = GUID.valueOf(args[0]);
        int userSlots = Integer.parseInt(args[1]);
        try (BusConnector connector = createConnector()) {
            try (YtClient client = new YtClient(connector, "hahn", getCredentials())){
                UpdateOperationParameters req = new UpdateOperationParameters(operation)
                        .setSchedulingOptionsPerPoolTree(Map.of(
                                "physical",
                                new SchedulingOptions().setResourceLimits(
                                        new ResourceLimits().setUserSlots(userSlots)
                                )
                        ));
                client.updateOperationParameters(req).join();
            }
        }
    }
}
