package ru.yandex.yt.ytclient.ytree;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.CodedInputStream;
import org.junit.Test;

import ru.yandex.yt.ytclient.yson.YsonBinaryDecoder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public class YTreeBuilderSubclassTest {
    private static class ListFragmentConsumer extends YTreeBuilder {
        private List<YTreeNode> values = new ArrayList<>();

        private class FragmentItems extends State {
            @Override
            public void onListItem() {
                // nothing
            }

            @Override
            public void onValue(YTreeNode value) {
                values.add(value);
            }
        }

        private ListFragmentConsumer() {
            push(new FragmentItems());
        }
    }

    @Test
    public void fragmentConsumer() throws IOException {
        // Проверяем, что можно легко написать подкласс YTreeBuilder'а, который
        // сможет что-либо делать с частями list fragment'а. В данном случае
        // они добавляются в список.
        ListFragmentConsumer consumer = new ListFragmentConsumer();
        YsonBinaryDecoder.parseListFragment(
                CodedInputStream.newInstance("42;{a=51};[1;2;3];".getBytes(StandardCharsets.UTF_8)),
                consumer);
        assertThat(consumer.values, contains(
                new YTreeInt64Node(42L),
                new YTreeBuilder().beginMap().key("a").value(51).buildMap(),
                new YTreeBuilder().beginList().value(1).value(2).value(3).buildList()));
    }
}
