package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.yt.ytclient.proxy.request.CreateNode;

@RunWith(value = Parameterized.class)
public class ReadWriteEntityTest extends ReadWriteTestBase {
    public ReadWriteEntityTest(YtClient yt) {
        super(yt);
    }

    @Test
    public void testEntityReadWriteInSkiff() throws Exception {
        testSkiffReadWrite(false);
    }

    @Test
    public void testEntityReadWriteInSkiffWithRetries() throws Exception {
        testSkiffReadWrite(true);
    }

    private void testSkiffReadWrite(boolean needRetries) throws Exception {
        ObjectsRowsGenerator generator = new ReadWriteTest.ObjectsRowsGenerator();

        YPath path = YPath.simple("//tmp/read-write-entity-test");

        yt.createNode(new CreateNode(path, CypressNodeType.TABLE).setForce(true)).join();

        TableWriter<ReadWriteTest.Row> writer = yt.writeTable(
                WriteTable.<ReadWriteTest.Row>builder()
                        .setPath(path)
                        .setSerializationContext(new SerializationContext<>(ReadWriteTest.Row.class))
                        .setNeedRetries(needRetries)
                        .build()
        ).join();

        List<Row> rows = generator.nextRows();

        while (rows != null) {
            while (rows != null && writer.write(rows)) {
                rows = generator.nextRows();
            }

            writer.readyEvent().join();
        }

        writer.close().join();

        TableReader<ReadWriteTest.Row> reader = yt.readTable(
                new ReadTable<>(path, ReadWriteTest.Row.class)
        ).join();

        List<Boolean> rowsSeen = new ArrayList<>();

        for (int i = 0; i < generator.rowsCount(); ++i) {
            rowsSeen.add(false);
        }

        int currentRowNumber = 0;

        while (reader.canRead()) {
            List<ReadWriteTest.Row> rowset;

            while ((rowset = reader.read()) != null) {
                for (ReadWriteTest.Row row : rowset) {
                    long intValue = row.intValue;

                    Assert.assertEquals(row.toString(), row.key, "key-" + intValue);
                    Assert.assertEquals(row.toString(), row.value, "value-" + intValue);

                    Assert.assertEquals(false, rowsSeen.get(currentRowNumber));
                    rowsSeen.set(currentRowNumber, true);

                    currentRowNumber += 1;
                }
            }
            reader.readyEvent().join();
        }

        reader.close().join();

        for (int i = 0; i < rowsSeen.size(); ++i) {
            Assert.assertTrue("row #" + i + " wasn't seen", rowsSeen.get(i));
        }
    }
}
