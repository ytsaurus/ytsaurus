import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;

import org.junit.Test;
import org.junit.Assert;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.inside.yt.kosher.Yt;
import ru.yandex.inside.yt.kosher.impl.YtUtils;


public class TestYtRecipe {
    @Test
    public void test() throws IOException {
        String ytProxy = System.getenv("YT_PROXY");
        Yt yt = YtUtils.http(ytProxy, "");

        YPath node = YPath.simple("//tmp/table");

        Assert.assertFalse(yt.cypress().exists(node));
        yt.cypress().create(node, CypressNodeType.TABLE);
        Assert.assertTrue(yt.cypress().exists(node));
    }
}
