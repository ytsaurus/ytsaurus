package tech.ytsaurus.client;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.operations.CommandSpec;
import tech.ytsaurus.client.operations.VanillaSpec;
import tech.ytsaurus.client.request.GetOperation;
import tech.ytsaurus.client.request.PatchOperationSpec;
import tech.ytsaurus.client.request.VanillaOperation;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class PatchOperationSpecTest extends YTsaurusClientTestBase {
    private static final long TEST_TIMEOUT = 30000;

    private YTsaurusClient ytClient;

    @Before
    public void setUp() {
        var ytFixture = createYtFixture();
        this.ytClient = ytFixture.getYt();
    }

    private GUID runOperation(int initialJobsCount, long testTimeout) throws ExecutionException, InterruptedException {
        return ytClient.startVanilla(VanillaOperation.builder()
                .setSpec(VanillaSpec.builder().setTasks(Map.of(
                                "task", CommandSpec.builder()
                                        .setCommand("sleep " + testTimeout / 1000)
                                        .setJobCount(initialJobsCount)
                                        .build()))
                        .build()).build()).get().getId();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void testPatchOperationJobsScaleUp() throws ExecutionException, InterruptedException {
        runPatchOperationSpecTest(2, 4);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void testPatchOperationJobsScaleDown() throws Exception {
        runPatchOperationSpecTest(2, 1);
    }

    private void runPatchOperationSpecTest(int initialJobsCount, int expectedJobsCount)
            throws ExecutionException, InterruptedException {
        GUID operationId = runOperation(initialJobsCount, TEST_TIMEOUT);
        waitUntilRunningOrTimeout(operationId, TEST_TIMEOUT);

        ytClient.patchOperationSpec(PatchOperationSpec.builder()
                .setOperationId(operationId)
                .addPatch(YPath.relative("/tasks/task/job_count"), YTree.builder().value(expectedJobsCount).build())
                .build());

        waitUntilJobsTotalEquals(operationId, expectedJobsCount, TEST_TIMEOUT);
    }

    private void waitUntilRunningOrTimeout(final GUID operationId, final long timeoutMillis)
            throws InterruptedException {
        waitUntilCondition(() -> isRunning(operationId), timeoutMillis);
    }

    private void waitUntilJobsTotalEquals(final GUID operationId, final int target, final long timeoutMillis)
            throws InterruptedException {
        waitUntilCondition(() -> getTotalJobsFromOperationSpec(operationId) == target, timeoutMillis);
    }

    private void waitUntilCondition(CheckCondition condition, long timeoutMillis) throws InterruptedException {
        long checkInterval = 1000L;
        long waited = 0L;
        while (waited < timeoutMillis) {
            try {
                if (condition.check()) {
                    return;
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(checkInterval);
            waited += checkInterval;
        }
    }

    private interface CheckCondition {
        boolean check();
    }

    private boolean isRunning(GUID operationId) {
        try {
            YTreeNode operation = getOperation(operationId, List.of("state"));
            String state = operation.mapNode().getString("state");
            return state.equals("running");
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to get 'state': " + ex.getMessage(), ex);
        }
    }

    private int getTotalJobsFromOperationSpec(GUID operationId) {
        try {
            YTreeNode operation = getOperation(operationId, List.of("brief_progress"));
            return operation.mapNode().getMap("brief_progress").mapNode().getMap("jobs").getInt("total");
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to get 'total' from brief_progress: " + ex.getMessage(), ex);
        }
    }

    private YTreeNode getOperation(GUID operationId, Collection<String> attributes) {
        try {
            return ytClient.getOperation(GetOperation.builder()
                    .setOperationId(operationId)
                    .setAttributes(attributes)
                    .build()).get();
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to get operation by id: ", ex);
        }
    }
}
