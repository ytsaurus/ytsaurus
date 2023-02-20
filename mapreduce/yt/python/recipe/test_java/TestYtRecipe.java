import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;

import org.junit.Test;
import org.junit.Assert;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ExistsNode;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

public class TestYtRecipe {
    @Test
    public void test() throws IOException {
        String ytProxy = System.getenv("YT_PROXY");
        YTsaurusClient yt = YTsaurusClient.builder()
                .setCluster(ytProxy)
                .setAuth(YTsaurusClientAuth.builder()
                        .setUser("root")
                        .setToken("")
                        .build())
                .build();

        YPath node = YPath.simple("//tmp/table");

        Assert.assertFalse(yt.existsNode(ExistsNode.builder().setPath(node).build()).join());
        yt.createNode(new CreateNode(node, CypressNodeType.TABLE)).join();
        Assert.assertTrue(yt.existsNode(ExistsNode.builder().setPath(node).build()).join());
    }
}
