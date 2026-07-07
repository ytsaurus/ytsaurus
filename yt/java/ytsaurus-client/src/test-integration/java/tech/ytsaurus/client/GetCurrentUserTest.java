package tech.ytsaurus.client;

import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.request.GetCurrentUser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GetCurrentUserTest extends YTsaurusClientTestBase {
    private static final long TEST_TIMEOUT = 30000;

    private YTsaurusClient ytClient;

    @Before
    public void setUp() {
        var ytFixture = createYtFixture();
        this.ytClient = ytFixture.getYt();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void testGetCurrentUser() throws ExecutionException, InterruptedException {
        String result = ytClient.getCurrentUser(GetCurrentUser.builder().build()).get();

        assertNotNull("Result should not be null", result);
        assertEquals("root", result);
    }
}
