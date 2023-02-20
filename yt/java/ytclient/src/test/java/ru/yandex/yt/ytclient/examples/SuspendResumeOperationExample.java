package ru.yandex.yt.ytclient.examples;

import tech.ytsaurus.client.YtClient;
import tech.ytsaurus.client.request.GetOperation;
import tech.ytsaurus.client.request.SuspendOperation;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeMapNode;

import ru.yandex.yt.ytclient.proxy.request.ResumeOperation;

import static ru.yandex.yt.ytclient.examples.ExamplesUtil.createConnector;
import static ru.yandex.yt.ytclient.examples.ExamplesUtil.getClientAuth;

public class SuspendResumeOperationExample {
    private SuspendResumeOperationExample() {
    }

    public static void main(String[] args) throws InterruptedException {
        var operation = GUID.valueOf(args[0]);
        var cluster = (args.length > 1) ? args[1] : "hahn";
        try (var connector = createConnector()) {
            try (var client = new YtClient(connector, cluster, getClientAuth())) {
                if (!isRunning(client, operation)) {
                    System.err.println("Operation " + operation + " is not running");
                    return;
                }
                if (!isSuspended(client, operation)) {
                    System.out.println("Operation is active, suspending");
                    suspendOperation(client, operation);
                } else {
                    System.out.println("Operation is suspended, resuming");
                    resumeOperation(client, operation);
                }
                System.out.println("Resulting operation state: suspended=" + isSuspended(client, operation));
            }
        }
    }

    private static YTreeMapNode getState(YtClient client, GUID operation) {
        var req = new GetOperation(operation);
        var res = client.getOperation(req).join();
        return res.mapNode();
    }

    private static boolean isRunning(YtClient client, GUID operation) {
        return getState(client, operation).getString("state").equals("running");
    }

    private static boolean isSuspended(YtClient client, GUID operation) {
        return getState(client, operation).getBoolO("suspended").stream()
                .anyMatch(s -> s);
    }

    private static void suspendOperation(YtClient client, GUID operation) throws InterruptedException {
        var req = SuspendOperation.builder()
                .setOperationId(operation)
                .setAbortRunningJobs(false)
                .build();
        client.suspendOperation(req).join();
        while (!isSuspended(client, operation)) {
            Thread.sleep(100);
        }
    }

    private static void resumeOperation(YtClient client, GUID operation) throws InterruptedException {
        var req = new ResumeOperation(operation);
        client.resumeOperation(req).join();
        while (isSuspended(client, operation)) {
            Thread.sleep(100);
        }
    }

}
