package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.YPath;

@RunWith(value = Parameterized.class)
public class ReadWriteComplexEntityTest extends ReadWriteTestBase {
    public ReadWriteComplexEntityTest(YtClient yt) {
        super(yt);
    }

    @Test
    public void testComplexEntityReadWriteInSkiff() throws Exception {
        YPath path = YPath.simple("//tmp/write-table-example-3");

        TableWriter<Product> writer = yt.writeTable(
                WriteTable.<Product>builder()
                        .setPath(path)
                        .setSerializationContext(
                                SerializationContext.skiff(Product.class)
                        )
                        .setNeedRetries(true)
                        .build()).join();

        List<Product> products = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            var product = new Product(i + "1",
                    new Statistics(i * 2, i * 3, new Info(i + "0")));
            if (writer.write(List.of(product))) {
                products.add(product);
            }
            writer.readyEvent().join();
        }

        writer.close().join();

        TableReader<Product> reader = yt.readTable(
                ReadTable.<Product>builder()
                        .setPath(path)
                        .setSerializationContext(
                                SerializationContext.skiff(Product.class)
                        )
                        .build()).join();

        int currentProductNumber = 0;

        while (reader.canRead()) {
            List<Product> productsFromTable;

            while ((productsFromTable = reader.read()) != null) {
                for (Product product : productsFromTable) {
                    Assert.assertEquals(product, products.get(currentProductNumber));
                    currentProductNumber++;
                }
            }
            reader.readyEvent().join();
        }

        reader.close().join();

        Assert.assertEquals(products.size(), currentProductNumber);
    }

    @Entity
    private static class Product {
        String name;
        Statistics statistics;

        Product() {
        }

        Product(String name, Statistics statistics) {
            this.name = name;
            this.statistics = statistics;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Product product = (Product) o;
            return Objects.equals(name, product.name) && Objects.equals(statistics, product.statistics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, statistics);
        }
    }

    @Entity
    private static class Statistics {
        int count;
        int size;
        Info info;

        Statistics() {
        }

        Statistics(int count, int size, Info info) {
            this.count = count;
            this.size = size;
            this.info = info;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Statistics that = (Statistics) o;
            return count == that.count && size == that.size && Objects.equals(info, that.info);
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, size, info);
        }
    }

    @Entity
    private static class Info {
        String data;

        Info() {
        }

        Info(String data) {
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Info info = (Info) o;
            return Objects.equals(data, info.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }
    }
}
