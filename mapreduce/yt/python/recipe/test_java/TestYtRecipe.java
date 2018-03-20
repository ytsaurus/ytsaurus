import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;

import org.junit.Test;
import org.junit.Assert;

import ru.yandex.inside.yt.kosher.Yt;
import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.YtUtils;


public class TestYtRecipe {
    @Test
    public void test() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("yt_proxy_port.txt"));
        String proxyPort = br.readLine();

        Yt yt = YtUtils.http("localhost:" + proxyPort, "");

        YPath node = YPath.simple("//tmp/table");

        Assert.assertFalse(yt.cypress().exists(node));
        yt.cypress().create(node, CypressNodeType.TABLE);
        Assert.assertTrue(yt.cypress().exists(node));
    }
}
