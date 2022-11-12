package ru.yandex.inside.yt.kosher.cypress;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import ru.yandex.inside.yt.kosher.common.GUID;

/**
 * @author sankear
 */
public class RichYPath implements YPath {

    private String rootDesignator;
    private List<String> relativePath;

    private Optional<Boolean> append;
    private Optional<Boolean> primary;
    private Optional<Boolean> foreign;
    private List<RangeCriteria> ranges;
    private List<String> columns;
    private Map<String, String> renameColumns;
    private List<String> sortedBy;
    private Optional<Long> timestamp;
    private Optional<YTreeNode> schema;
    private Optional<String> format;
    private Optional<Boolean> bypassArtifactCache;
    private Optional<Boolean> executable;

    private Map<String, YTreeNode> additionalAttributes;

    RichYPath(String rootDesignator, List<String> relativePath) {
        this.rootDesignator = rootDesignator;
        this.relativePath = relativePath;

        this.append = Optional.empty();
        this.primary = Optional.empty();
        this.foreign = Optional.empty();
        this.ranges = Collections.emptyList();
        this.columns = Collections.emptyList();
        this.renameColumns = Collections.emptyMap();
        this.sortedBy = Collections.emptyList();
        this.timestamp = Optional.empty();
        this.schema = Optional.empty();
        this.format = Optional.empty();
        this.bypassArtifactCache = Optional.empty();
        this.executable = Optional.empty();

        this.additionalAttributes = Collections.emptyMap();
    }

    private RichYPath(RichYPath other) {
        this.rootDesignator = other.rootDesignator;
        this.relativePath = other.relativePath;

        this.append = other.append;
        this.primary = other.primary;
        this.ranges = other.ranges;
        this.columns = other.columns;
        this.renameColumns = other.renameColumns;
        this.foreign = other.foreign;
        this.sortedBy = other.sortedBy;
        this.timestamp = other.timestamp;
        this.schema = other.schema;
        this.format = other.format;
        this.bypassArtifactCache = other.bypassArtifactCache;
        this.executable = other.executable;

        this.additionalAttributes = other.additionalAttributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RichYPath richYPath = (RichYPath) o;
        return Objects.equals(rootDesignator, richYPath.rootDesignator)
                && Objects.equals(relativePath, richYPath.relativePath)
                && Objects.equals(append, richYPath.append)
                && Objects.equals(primary, richYPath.primary)
                && Objects.equals(foreign, richYPath.foreign)
                && Objects.equals(ranges, richYPath.ranges)
                && Objects.equals(columns, richYPath.columns)
                && Objects.equals(renameColumns, richYPath.renameColumns)
                && Objects.equals(sortedBy, richYPath.sortedBy)
                && Objects.equals(timestamp, richYPath.timestamp)
                && Objects.equals(schema, richYPath.schema)
                && Objects.equals(format, richYPath.format)
                && Objects.equals(bypassArtifactCache, richYPath.bypassArtifactCache)
                && Objects.equals(executable, richYPath.executable)
                && Objects.equals(additionalAttributes, richYPath.additionalAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rootDesignator,
                relativePath,
                append,
                primary,
                foreign,
                ranges,
                columns,
                renameColumns,
                sortedBy,
                timestamp,
                schema,
                format,
                bypassArtifactCache,
                executable,
                additionalAttributes
        );
    }

    /**
     * @return path without RichPath additional attributes
     */
    @Override
    public YPath justPath() {
        return new RichYPath(rootDesignator, relativePath);
    }

    @Override
    public String name() {
        if (isAttribute()) {
            throw new IllegalStateException();
        }
        return relativePath.get(relativePath.size() - 1);
    }

    @Override
    public boolean isRoot() {
        return relativePath.isEmpty();
    }

    @Override
    public boolean isAttribute() {
        return !relativePath.isEmpty() && relativePath.get(relativePath.size() - 1).startsWith("@");
    }

    @Override
    public boolean hasObjectRootDesignator() {
        return rootDesignator.startsWith("#");
    }

    @Override
    public YPath withObjectRoot(GUID id) {
        RichYPath copy = new RichYPath(this);
        copy.rootDesignator = "#" + id;
        copy.relativePath = Collections.emptyList();
        return copy;
    }

    @Override
    public YPath parent() {
        if (relativePath.isEmpty()) {
            return this;
        }

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(relativePath.subList(0, relativePath.size() - 1));
        return copy;
    }

    @Override
    public YPath child(String key) {
        List<String> list = new ArrayList<>(relativePath);
        list.addAll(getRelativePath("/".concat(key), 0));

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(list);
        return copy;
    }

    @Override
    public YPath after(int index) {
        List<String> list = new ArrayList<>(relativePath);
        list.add("after:" + index);

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(list);
        return copy;
    }

    @Override
    public YPath before(int index) {
        List<String> list = new ArrayList<>(relativePath);
        list.add("before:" + index);

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(list);
        return copy;
    }

    @Override
    public YPath begin() {
        List<String> list = new ArrayList<>(relativePath);
        list.add("begin");

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(list);
        return copy;
    }

    @Override
    public YPath end() {
        List<String> list = new ArrayList<>(relativePath);
        list.add("end");

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(list);
        return copy;
    }

    @Override
    public YPath child(int index) {
        List<String> list = new ArrayList<>(relativePath);
        list.add(String.valueOf(index));

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(list);
        return copy;
    }

    @Override
    public YPath attribute(String key) {
        List<String> list = new ArrayList<>(relativePath);
        list.add("@" + key);

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(list);
        return copy;
    }

    @Override
    public YPath all() {
        List<String> list = new ArrayList<>(relativePath);
        list.add("*");

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(list);
        return copy;
    }

    @Override
    public YPath allAttributes() {
        List<String> list = new ArrayList<>(relativePath);
        list.add("@");

        RichYPath copy = new RichYPath(this);
        copy.relativePath = Collections.unmodifiableList(list);
        return copy;
    }

    @Override
    public Optional<Boolean> getAppend() {
        return append;
    }

    @Override
    public YPath append(boolean append) {
        RichYPath copy = new RichYPath(this);
        copy.append = Optional.of(append);
        return copy;
    }

    @Override
    public Optional<Boolean> getPrimary() {
        return primary;
    }

    @Override
    public YPath primary(boolean primary) {
        RichYPath copy = new RichYPath(this);
        copy.primary = Optional.of(primary);
        return copy;
    }

    @Override
    public Optional<Boolean> getForeign() {
        return foreign;
    }

    @Override
    public YPath foreign(boolean foreign) {
        RichYPath copy = new RichYPath(this);
        copy.foreign = Optional.of(foreign);
        return copy;
    }

    @Override
    public Optional<YTreeNode> getSchema() {
        return schema;
    }

    @Override
    public YPath withSchema(YTreeNode schema) {
        RichYPath copy = new RichYPath(this);
        copy.schema = Optional.of(schema);
        return copy;
    }

    @Override
    public List<RangeCriteria> getRanges() {
        return ranges;
    }

    @Override
    public YPath ranges(List<RangeCriteria> ranges) {
        RichYPath copy = new RichYPath(this);
        copy.ranges = Collections.unmodifiableList(new ArrayList<>(ranges));
        return copy;
    }

    @Override
    public YPath plusRange(RangeCriteria range) {
        List<RangeCriteria> list = new ArrayList<>(ranges);
        list.add(range);

        RichYPath copy = new RichYPath(this);
        copy.ranges = Collections.unmodifiableList(new ArrayList<>(list));
        return copy;
    }

    @Override
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public YPath withColumns(Collection<String> columns) {
        RichYPath copy = new RichYPath(this);
        copy.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        return copy;
    }

    @Override
    public Map<String, String> getRenameColumns() {
        return renameColumns;
    }

    @Override
    public YPath withRenameColumns(Map<String, String> renameColumns) {
        RichYPath copy = new RichYPath(this);
        copy.renameColumns = Collections.unmodifiableMap(new HashMap<>(renameColumns));
        return copy;
    }

    @Override
    public YPath plusRenameColumns(Map<String, String> renameColumns) {
        Map<String, String> map = new HashMap<>(this.renameColumns);
        map.putAll(renameColumns);

        RichYPath copy = new RichYPath(this);
        copy.renameColumns = Collections.unmodifiableMap(map);
        return copy;
    }

    @Override
    public List<String> getSortedBy() {
        return sortedBy;
    }

    @Override
    public YPath sortedBy(List<String> sortedBy) {
        RichYPath copy = new RichYPath(this);
        copy.sortedBy = Collections.unmodifiableList(new ArrayList<>(sortedBy));
        return copy;
    }

    @Override
    public Optional<Long> getTimestamp() {
        return timestamp;
    }

    @Override
    public YPath withTimestamp(long timestamp) {
        RichYPath copy = new RichYPath(this);
        copy.timestamp = Optional.of(timestamp);
        return copy;
    }

    @Override
    public Optional<String> getFormat() {
        return format;
    }

    @Override
    public YPath withFormat(String format) {
        RichYPath copy = new RichYPath(this);
        copy.format = Optional.of(format);
        return copy;
    }

    @Override
    public Optional<Boolean> getExecutable() {
        return executable;
    }

    @Override
    public YPath withExecutable(boolean executable) {
        RichYPath copy = new RichYPath(this);
        copy.executable = Optional.of(executable);
        return copy;
    }

    @Override
    public Optional<YTreeNode> getAdditionalAttribute(String attributeName) {
        return Optional.ofNullable(additionalAttributes.get(attributeName));
    }

    @Override
    public Map<String, YTreeNode> getAdditionalAttributes() {
        return additionalAttributes;
    }

    @Override
    public YPath withAdditionalAttributes(Map<String, YTreeNode> additionalAttributes) {
        RichYPath copy = new RichYPath(this);
        copy.additionalAttributes = Collections.unmodifiableMap(new HashMap<>(additionalAttributes));
        return copy;
    }

    @Override
    public YPath plusAdditionalAttribute(String key, YTreeNode value) {
        Map<String, YTreeNode> map = new HashMap<>(additionalAttributes);
        map.put(key, value);

        RichYPath copy = new RichYPath(this);
        copy.additionalAttributes = Collections.unmodifiableMap(map);
        return copy;
    }

    @Override
    public Optional<Boolean> getBypassArtifactCache() {
        return bypassArtifactCache;
    }

    @Override
    public YPath withBypassArtifactCache(boolean bypassArtifactCache) {
        RichYPath copy = new RichYPath(this);
        copy.bypassArtifactCache = Optional.of(bypassArtifactCache);
        return copy;
    }

    @Override
    public String toString() {
        String simpleString = Stream.concat(Stream.of(rootDesignator), relativePath.stream())
                .collect(Collectors.joining("/"));
        YTreeNode node = toTree();
        if (node.containsAttributes()) {
            String tmp = YTreeTextSerializer.serialize(buildAttributes(YTree.builder()).value(31337).build());
            return tmp.substring(0, tmp.length() - 5) + simpleString;
        } else {
            return simpleString;
        }
    }

    @Override
    public YTreeNode toTree() {
        return toTree(YTree.builder()).build();
    }

    private YTreeBuilder buildAttributes(YTreeBuilder builder) {
        return builder
                .beginAttributes()
                .when(append.isPresent(), b -> b.key("append").value(append.get()))
                .when(primary.isPresent(), b -> b.key("primary").value(primary.get()))
                .when(!ranges.isEmpty(), b -> b.key("ranges").value(ranges, (b2, r) -> {
                    YTreeBuilder rangesBuilder = b2.beginMap();
                    rangesBuilder = r.addRangeCriteria(rangesBuilder);
                    return rangesBuilder.endMap();
                }))
                .when(foreign.isPresent(), b -> b.key("foreign").value(foreign.get()))
                .when(!columns.isEmpty(), b -> b.key("columns").value(columns))
                .when(!renameColumns.isEmpty(), b -> {
                    YTreeBuilder mapBuilder = YTree.mapBuilder();
                    for (Map.Entry<String, String> oldToNewColumn : renameColumns.entrySet()) {
                        mapBuilder.key(oldToNewColumn.getKey()).value(oldToNewColumn.getValue());
                    }
                    return b.key("rename_columns").value(mapBuilder.buildMap());
                })
                .when(!sortedBy.isEmpty(), b -> b.key("sorted_by").value(sortedBy))
                .when(timestamp.isPresent(), b -> b.key("timestamp").value(timestamp.get()))
                .when(schema.isPresent(), b -> b.key("schema").value(schema.get()))
                .when(format.isPresent(), b -> b.key("format").value(format.get()))
                .when(bypassArtifactCache.isPresent(),
                        b -> b.key("bypass_artifact_cache").value(bypassArtifactCache.get()))
                .when(executable.isPresent(), b -> b.key("executable").value(executable.get()))
                .apply(b -> {
                    for (Map.Entry<String, YTreeNode> node : additionalAttributes.entrySet()) {
                        b.key(node.getKey()).value(node.getValue());
                    }
                    return b;
                })
                .endAttributes();
    }

    @Override
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return buildAttributes(builder)
                .value(Stream.concat(Stream.of(rootDesignator), relativePath.stream())
                        .collect(Collectors.joining("/")));
    }

    public static YPath cypressRoot() {
        return new RichYPath("/", Collections.emptyList());
    }

    public static YPath objectRoot(GUID id) {
        return new RichYPath("#" + id.toString(), Collections.emptyList());
    }

    public static YPath simple(String path) {
        return getRootDesignatorAndRelativePath(path);
    }

    public static YPath fromTree(YTreeNode node) {
        String path = node.stringValue();
        Map<String, YTreeNode> attributes = node.getAttributes();

        YPath simplePath = getRootDesignatorAndRelativePath(path);
        if (attributes.isEmpty()) {
            return simplePath;
        } else {
            return RichYPath.fromAttributes(simplePath, attributes);
        }
    }

    private static YPath getRootDesignatorAndRelativePath(String path) {
        if (path.startsWith("/")) {
            return new RichYPath("/", getRelativePath(path, 1));
        } else if (path.startsWith("#")) {
            int ptr = 0;
            while (ptr < path.length() && path.charAt(ptr) != '/') {
                ++ptr;
            }
            String guid = path.substring(1, ptr);
            if (!GUID.isValid(guid)) {
                throw new IllegalArgumentException(path);
            }
            return new RichYPath("#" + guid, getRelativePath(path, ptr));
        } else {
            throw new IllegalArgumentException(path);
        }
    }

    private static List<String> getRelativePath(String path, int ptr) {
        if (ptr >= path.length()) {
            return Collections.emptyList();
        }
        if (path.charAt(ptr) != '/') {
            throw new IllegalStateException();
        }
        List<String> result = new ArrayList<>();
        while (ptr < path.length()) {
            int nextPtr = ptr + 1;
            while (nextPtr < path.length() && path.charAt(nextPtr) != '/') {
                ++nextPtr;
            }
            if (nextPtr == ptr + 1) {
                throw new IllegalArgumentException(path);
            }
            result.add(path.substring(ptr + 1, nextPtr));
            ptr = nextPtr;
        }
        return Collections.unmodifiableList(result);
    }

    private static YPath fromAttributes(YPath simplePath, Map<String, YTreeNode> attrs) {
        Map<String, YTreeNode> attributes = new HashMap<>(attrs);

        Optional<Boolean> append = Optional.ofNullable(attributes.remove("append")).map(YTreeNode::boolValue);
        Optional<Boolean> primary = Optional.ofNullable(attributes.remove("primary")).map(YTreeNode::boolValue);
        Optional<Boolean> foreign = Optional.ofNullable(attributes.remove("foreign")).map(YTreeNode::boolValue);
        Optional<Boolean> bypassArtifactCache = Optional.ofNullable(attributes.remove("bypass_artifact_cache"))
                .map(YTreeNode::boolValue);
        Optional<Boolean> executable = Optional.ofNullable(attributes.remove("executable")).map(YTreeNode::boolValue);

        List<RangeCriteria> ranges = new ArrayList<>();
        Optional<YTreeNode> rangesAttribute = Optional.ofNullable(attributes.remove("ranges"));
        if (rangesAttribute.isPresent()) {
            for (YTreeNode range : rangesAttribute.get().listNode()) {
                Optional<RangeLimit> lower = getLimit(range, "lower_limit");
                Optional<RangeLimit> upper = getLimit(range, "upper_limit");
                if (lower.isPresent() && upper.isPresent()) {
                    ranges.add(new Range(lower.get(), upper.get()));
                }

                Optional<RangeLimit> exact = getLimit(range, "exact");
                exact.ifPresent(rangeLimit -> ranges.add(new Exact(rangeLimit)));
            }
        }
        Set<String> columns = Optional.ofNullable(attributes.remove("columns"))
                .map(v -> v.asList().stream()).orElse(Stream.of())
                .map(YTreeNode::stringValue)
                .collect(Collectors.toSet());
        Map<String, String> renameColumns = Optional.ofNullable(attributes.remove("rename_columns"))
                .map(v -> v.asMap().entrySet().stream()).orElse(Stream.of())
                .collect(Collectors.toMap(v -> v.getKey(), v -> v.getValue().stringValue()));
        List<String> sortedBy = Optional.ofNullable(attributes.remove("sorted_by"))
                .map(v -> v.asList().stream()).orElse(Stream.of())
                .map(YTreeNode::stringValue)
                .collect(Collectors.toList());
        Optional<Long> timestamp = Optional.ofNullable(attributes.remove("timestamp")).map(YTreeNode::longValue);
        Optional<YTreeNode> schema = Optional.ofNullable(attributes.remove("schema"));

        YPath result = simplePath;
        if (append.isPresent()) {
            result = result.append(append.get());
        }
        if (primary.isPresent()) {
            result = result.primary(primary.get());
        }
        if (foreign.isPresent()) {
            result = result.foreign(foreign.get());
        }
        if (!ranges.isEmpty()) {
            result = result.ranges(ranges);
        }
        if (!columns.isEmpty()) {
            result = result.withColumns(columns);
        }
        if (!renameColumns.isEmpty()) {
            result = result.withRenameColumns(renameColumns);
        }
        if (!sortedBy.isEmpty()) {
            result = result.sortedBy(sortedBy);
        }
        if (timestamp.isPresent()) {
            result = result.withTimestamp(timestamp.get());
        }
        if (schema.isPresent()) {
            result = result.withSchema(schema.get());
        }
        if (bypassArtifactCache.isPresent()) {
            result = result.withBypassArtifactCache(bypassArtifactCache.get());
        }
        if (executable.isPresent()) {
            result = result.withExecutable(executable.get());
        }

        if (!attributes.isEmpty()) {
            result = result.withAdditionalAttributes(attributes);
        }
        return result;
    }

    private static Optional<RangeLimit> getLimit(YTreeNode node, String key) {
        final YTreeMapNode parentMapNode = node.mapNode();
        if (!parentMapNode.containsKey(key)) {
            return Optional.empty();
        }
        YTreeMapNode mapNode = parentMapNode.getOrThrow(key).mapNode();
        List<YTreeNode> limitKey = new ArrayList<>();
        if (mapNode.containsKey("key")) {
            limitKey = mapNode.getOrThrow("key").asList();
        }
        long rowIndex = -1;
        if (mapNode.containsKey("row_index")) {
            rowIndex = mapNode.getOrThrow("row_index").longValue();
        }
        long offset = -1;
        if (mapNode.containsKey("offset")) {
            offset = mapNode.getOrThrow("offset").longValue();
        }
        return Optional.of(RangeLimit.builder().setKey(limitKey).setRowIndex(rowIndex).setOffset(offset).build());
    }
}

