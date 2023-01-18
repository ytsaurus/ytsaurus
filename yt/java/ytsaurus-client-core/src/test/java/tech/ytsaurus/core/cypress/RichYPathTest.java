package tech.ytsaurus.core.cypress;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.ysontree.YTree;

public class RichYPathTest {

    private static final List<RangeCriteria> DEFAULT_RANGES = List.of(new Range(RangeLimit.row(1),
            RangeLimit.builder()
                    .setKey(List.of(YTree.stringNode("k1"), YTree.stringNode("k2")))
                    .setRowIndex(123)
                    .setOffset(43)
                    .build()));
    private static final List<String> DEFAULT_COLUMNS = List.of("c1", "c2");
    private static final List<String> DEFAULT_RELATIVE_PATH = List.of("home", "sankear", "table");
    private static final List<String> DEFAULT_SORTED_BY = List.of("c3", "c4");
    private static final long DEFAULT_TIMESTAMP = 123;

    private RichYPath defaultPath() {
        return (RichYPath) new RichYPath("/", DEFAULT_RELATIVE_PATH)
                .append(true)
                .primary(false)
                .foreign(false)
                .ranges(DEFAULT_RANGES)
                .withColumns(DEFAULT_COLUMNS)
                .sortedBy(DEFAULT_SORTED_BY)
                .withTimestamp(DEFAULT_TIMESTAMP);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private YPath path(boolean append, boolean primary,
                       List<RangeCriteria> ranges, List<String> columns, String rootDesignator,
                       List<String> relativePath, boolean foreign, List<String> sortedBy, long timestamp) {
        return new RichYPath(rootDesignator, relativePath)
                .append(append)
                .primary(primary)
                .foreign(foreign)
                .ranges(ranges)
                .withColumns(columns)
                .sortedBy(sortedBy)
                .withTimestamp(timestamp);
    }

    @Test
    public void parent() {
        Assert.assertEquals(path(
                true, false, DEFAULT_RANGES, DEFAULT_COLUMNS,
                        "/", Arrays.asList("home", "sankear"), false,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().parent());
    }

    @Test
    public void child() {
        Assert.assertEquals(path(true, false, DEFAULT_RANGES, DEFAULT_COLUMNS, "/",
                Arrays.asList("home", "sankear", "table", "2015-11-25"), false, DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().child("2015-11-25"));
    }

    @Test
    public void after() {
        Assert.assertEquals(path(true, false, DEFAULT_RANGES, DEFAULT_COLUMNS, "/",
                Arrays.asList("home", "sankear", "table", "after:12"), false, DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().after(12));
    }

    @Test
    public void before() {
        Assert.assertEquals(path(true, false, DEFAULT_RANGES, DEFAULT_COLUMNS, "/",
                Arrays.asList("home", "sankear", "table", "before:4243"), false, DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().before(4243));
    }

    @Test
    public void begin() {
        Assert.assertEquals(path(true, false,
                DEFAULT_RANGES, DEFAULT_COLUMNS, "/", Arrays.asList("home", "sankear", "table", "begin"), false,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().begin());
    }

    @Test
    public void end() {
        Assert.assertEquals(path(true, false,
                DEFAULT_RANGES, DEFAULT_COLUMNS, "/", Arrays.asList("home", "sankear", "table", "end"), false,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().end());
    }

    @Test
    public void childIndex() {
        Assert.assertEquals(path(true, false,
                DEFAULT_RANGES, DEFAULT_COLUMNS, "/", Arrays.asList("home", "sankear", "table", "18"), false,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().child(18));
    }

    @Test
    public void attribute() {
        Assert.assertEquals(path(true, false,
                DEFAULT_RANGES, DEFAULT_COLUMNS, "/", Arrays.asList("home", "sankear", "table", "@id"), false,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().attribute("id"));
    }

    @Test
    public void all() {
        Assert.assertEquals(path(true, false,
                DEFAULT_RANGES, DEFAULT_COLUMNS, "/", Arrays.asList("home", "sankear", "table", "*"), false,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().all());
    }

    @Test
    public void allAttributes() {
        Assert.assertEquals(path(true, false,
                DEFAULT_RANGES, DEFAULT_COLUMNS, "/", Arrays.asList("home", "sankear", "table", "@"), false,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().allAttributes());
    }

    @Test
    public void append() {
        Assert.assertEquals(path(false, false, DEFAULT_RANGES, DEFAULT_COLUMNS, "/", DEFAULT_RELATIVE_PATH, false,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().append(false));
    }

    @Test
    public void primary() {
        Assert.assertEquals(path(true, true, DEFAULT_RANGES, DEFAULT_COLUMNS, "/", DEFAULT_RELATIVE_PATH, false,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().primary(true));
    }

    @Test
    public void foreign() {
        Assert.assertEquals(path(true, false, DEFAULT_RANGES, DEFAULT_COLUMNS, "/", DEFAULT_RELATIVE_PATH, true,
                DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().foreign(true));
    }

    @Test
    public void withRange() {
        Assert.assertEquals(path(
                true, false,
                Stream.concat(DEFAULT_RANGES.stream(), Stream.of(new Range(RangeLimit.row(19), RangeLimit.row(100))))
                        .collect(Collectors.toList()),
                DEFAULT_COLUMNS, "/", DEFAULT_RELATIVE_PATH, false,
                        DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().withRange(RangeLimit.row(19), RangeLimit.row(100)));
    }

    @Test
    public void withExact() {
        final RangeLimit exact = RangeLimit.key(YTree.stringNode("aaa"));
        Assert.assertEquals(path(true, false, Stream.concat(DEFAULT_RANGES.stream(), Stream.of(new Exact(
                        exact))).collect(Collectors.toList()),
                DEFAULT_COLUMNS, "/", DEFAULT_RELATIVE_PATH, false, DEFAULT_SORTED_BY, DEFAULT_TIMESTAMP),
                defaultPath().withExact(exact));
    }

    @Test
    public void withColumns() {
        Assert.assertEquals(path(true, false,
                DEFAULT_RANGES, Stream.concat(DEFAULT_COLUMNS.stream(), Stream.of("c12"))
                                .collect(Collectors.toList()),
                "/", DEFAULT_RELATIVE_PATH, false, DEFAULT_SORTED_BY,
                DEFAULT_TIMESTAMP),
                defaultPath().withColumns("c1", "c2", "c12"));
    }

    @Test
    public void testToString() {
        Assert.assertEquals(defaultPath().toString(),
                "<\"ranges\"=[{\"lower_limit\"={\"row_index\"=1;};\"upper_limit\"={\"offset\"=43;\"row_index\"=123;" +
                        "\"key\"=[\"k1\";\"k2\";];};};];\"columns\"=[\"c1\";\"c2\";];\"append\"=%true;" +
                        "\"sorted_by\"=[\"c3\";\"c4\";];\"primary\"=%false;\"foreign\"=%false;" +
                        "\"timestamp\"=123;>//home/sankear/table");
    }

    @Test
    public void sortedBy() {
        Assert.assertEquals(path(true, false,
                DEFAULT_RANGES, DEFAULT_COLUMNS, "/", DEFAULT_RELATIVE_PATH, false, List.of("c12"),
                DEFAULT_TIMESTAMP),
                defaultPath().sortedBy("c12"));
    }

    @Test
    public void withTimestamp() {
        YPath expected = path(true, false, DEFAULT_RANGES, DEFAULT_COLUMNS, "/", DEFAULT_RELATIVE_PATH, false,
                DEFAULT_SORTED_BY, 456);
        Assert.assertEquals(expected, defaultPath().withTimestamp(456));
    }

    @Test
    public void toTree() {
        Assert.assertEquals(defaultPath(), YPath.fromTree(defaultPath().toTree()));
        YPath path = path(true, false, List.of(), List.of(), "/", DEFAULT_RELATIVE_PATH, false, DEFAULT_SORTED_BY,
                DEFAULT_TIMESTAMP);
        Assert.assertEquals(path, YPath.fromTree(path.toTree()));
        path = path(true, false, List.of(), List.of("c1", "c2"), "/", DEFAULT_RELATIVE_PATH, false,
                DEFAULT_SORTED_BY,
                DEFAULT_TIMESTAMP);
        Assert.assertEquals(path, YPath.fromTree(path.toTree()));
        path = path(true, true, List.of(), List.of("c1", "c2"), "/", DEFAULT_RELATIVE_PATH, false,
                DEFAULT_SORTED_BY,
                DEFAULT_TIMESTAMP);
        Assert.assertEquals(path, YPath.fromTree(path.toTree()));
    }
}
