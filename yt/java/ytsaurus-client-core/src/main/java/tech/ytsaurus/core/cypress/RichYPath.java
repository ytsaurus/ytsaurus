package tech.ytsaurus.core.cypress;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.yson.YsonTokenType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;


@NonNullFields
public class RichYPath implements YPath {

    private String rootDesignator;
    private List<String> relativePath;

    private @Nullable Boolean append;
    private @Nullable Boolean primary;
    private @Nullable Boolean foreign;
    private List<RangeCriteria> ranges;
    private List<String> columns;
    private Map<String, String> renameColumns;
    private List<String> sortedBy;
    private @Nullable Long timestamp;
    private @Nullable YTreeNode schema;
    private @Nullable String format;
    private @Nullable Boolean bypassArtifactCache;
    private @Nullable Boolean executable;
    private @Nullable Boolean create;

    private Map<String, YTreeNode> additionalAttributes;

    RichYPath(String rootDesignator, List<String> relativePath) {
        this.rootDesignator = rootDesignator;
        this.relativePath = relativePath;
        this.append = null;
        this.primary = null;
        this.foreign = null;
        this.ranges = List.of();
        this.columns = List.of();
        this.renameColumns = Map.of();
        this.sortedBy = List.of();
        this.timestamp = null;
        this.schema = null;
        this.format = null;
        this.bypassArtifactCache = null;
        this.executable = null;
        this.create = null;

        this.additionalAttributes = Map.of();
    }

    private RichYPath(RichYPath other, List<String> relativePath) {
        this.rootDesignator = other.rootDesignator;
        this.relativePath = relativePath;

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
        this.create = other.create;

        this.additionalAttributes = other.additionalAttributes;
    }

    private RichYPath(RichYPath other) {
        this(other, other.relativePath);
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
                && Objects.equals(create, richYPath.create)
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
                create,
                additionalAttributes
        );
    }

    /**
     * Get path without RichPath additional attributes.
     */
    @Override
    public YPath justPath() {
        return new RichYPath(rootDesignator, relativePath);
    }

    /**
     * Get last component of the path (e.g. `//foo/bar` --- `bar`).
     */
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
        copy.relativePath = List.of();
        return copy;
    }

    @Override
    public YPath parent() {
        if (relativePath.isEmpty()) {
            return this;
        }
        List<String> newRelativePath = List.copyOf(relativePath.subList(0, relativePath.size() - 1));
        return new RichYPath(this, newRelativePath);
    }

    @Override
    public YPath child(String key) {
        List<String> list = new ArrayList<>(relativePath);
        list.addAll(getRelativePath("/".concat(key), 0));

        List<String> newRelativePath = List.copyOf(list);
        return new RichYPath(this, newRelativePath);
    }

    @Override
    public YPath after(int index) {
        List<String> list = new ArrayList<>(relativePath);
        list.add("after:" + index);

        List<String> newRelativePath = List.copyOf(list);
        return new RichYPath(this, newRelativePath);
    }

    @Override
    public YPath before(int index) {
        List<String> list = new ArrayList<>(relativePath);
        list.add("before:" + index);

        List<String> newRelativePath = List.copyOf(list);
        return new RichYPath(this, newRelativePath);
    }

    @Override
    public YPath begin() {
        List<String> list = new ArrayList<>(relativePath);
        list.add("begin");

        List<String> newRelativePath = List.copyOf(list);
        return new RichYPath(this, newRelativePath);
    }

    @Override
    public YPath end() {
        List<String> list = new ArrayList<>(relativePath);
        list.add("end");

        List<String> newRelativePath = List.copyOf(list);
        return new RichYPath(this, newRelativePath);
    }

    @Override
    public YPath child(int index) {
        List<String> list = new ArrayList<>(relativePath);
        list.add(String.valueOf(index));

        RichYPath copy = new RichYPath(this);
        copy.relativePath = List.copyOf(list);
        return copy;
    }

    @Override
    public YPath attribute(String key) {
        List<String> list = new ArrayList<>(relativePath);
        list.add("@" + key);

        RichYPath copy = new RichYPath(this);
        copy.relativePath = List.copyOf(list);
        return copy;
    }

    @Override
    public YPath all() {
        List<String> list = new ArrayList<>(relativePath);
        list.add("*");

        RichYPath copy = new RichYPath(this);
        copy.relativePath = List.copyOf(list);
        return copy;
    }

    @Override
    public YPath allAttributes() {
        List<String> list = new ArrayList<>(relativePath);
        list.add("@");

        RichYPath copy = new RichYPath(this);
        copy.relativePath = List.copyOf(list);
        return copy;
    }

    @Override
    public Optional<Boolean> getAppend() {
        return Optional.ofNullable(append);
    }

    @Override
    public YPath append(boolean append) {
        RichYPath copy = new RichYPath(this);
        copy.append = append;
        return copy;
    }

    @Override
    public Optional<Boolean> getPrimary() {
        return Optional.ofNullable(primary);
    }

    @Override
    public YPath primary(boolean primary) {
        RichYPath copy = new RichYPath(this);
        copy.primary = primary;
        return copy;
    }

    @Override
    public Optional<Boolean> getForeign() {
        return Optional.ofNullable(foreign);
    }

    @Override
    public YPath foreign(boolean foreign) {
        RichYPath copy = new RichYPath(this);
        copy.foreign = foreign;
        return copy;
    }

    @Override
    public Optional<YTreeNode> getSchema() {
        return Optional.ofNullable(schema);
    }

    @Override
    public YPath withSchema(YTreeNode schema) {
        RichYPath copy = new RichYPath(this);
        copy.schema = schema;
        return copy;
    }

    @Override
    public List<RangeCriteria> getRanges() {
        return ranges;
    }

    @Override
    public YPath ranges(List<RangeCriteria> ranges) {
        RichYPath copy = new RichYPath(this);
        copy.ranges = List.copyOf(new ArrayList<>(ranges));
        return copy;
    }

    @Override
    public YPath plusRange(RangeCriteria range) {
        List<RangeCriteria> list = new ArrayList<>(ranges);
        list.add(range);

        RichYPath copy = new RichYPath(this);
        copy.ranges = List.copyOf(new ArrayList<>(list));
        return copy;
    }

    @Override
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public YPath withColumns(Collection<String> columns) {
        RichYPath copy = new RichYPath(this);
        copy.columns = List.copyOf(new ArrayList<>(columns));
        return copy;
    }

    @Override
    public Map<String, String> getRenameColumns() {
        return renameColumns;
    }

    @Override
    public YPath withRenameColumns(Map<String, String> renameColumns) {
        RichYPath copy = new RichYPath(this);
        copy.renameColumns = Map.copyOf(renameColumns);
        return copy;
    }

    @Override
    public YPath plusRenameColumns(Map<String, String> renameColumns) {
        Map<String, String> map = new HashMap<>(this.renameColumns);
        map.putAll(renameColumns);

        RichYPath copy = new RichYPath(this);
        copy.renameColumns = Map.copyOf(map);
        return copy;
    }

    @Override
    public List<String> getSortedBy() {
        return sortedBy;
    }

    @Override
    public YPath sortedBy(List<String> sortedBy) {
        RichYPath copy = new RichYPath(this);
        copy.sortedBy = List.copyOf(new ArrayList<>(sortedBy));
        return copy;
    }

    @Override
    public Optional<Long> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    @Override
    public YPath withTimestamp(long timestamp) {
        RichYPath copy = new RichYPath(this);
        copy.timestamp = timestamp;
        return copy;
    }

    @Override
    public Optional<String> getFormat() {
        return Optional.ofNullable(format);
    }

    @Override
    public YPath withFormat(String format) {
        RichYPath copy = new RichYPath(this);
        copy.format = format;
        return copy;
    }

    @Override
    public Optional<Boolean> getExecutable() {
        return Optional.ofNullable(executable);
    }

    @Override
    public YPath withExecutable(boolean executable) {
        RichYPath copy = new RichYPath(this);
        copy.executable = executable;
        return copy;
    }

    @Override
    public Optional<Boolean> getCreate() {
        return Optional.ofNullable(create);
    }

    @Override
    public YPath create(boolean create) {
        RichYPath copy = new RichYPath(this);
        copy.create = create;
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
        copy.additionalAttributes = Map.copyOf(additionalAttributes);
        return copy;
    }

    @Override
    public YPath plusAdditionalAttribute(String key, YTreeNode value) {
        Map<String, YTreeNode> map = new HashMap<>(additionalAttributes);
        map.put(key, value);

        RichYPath copy = new RichYPath(this);
        copy.additionalAttributes = Map.copyOf(map);
        return copy;
    }

    @Override
    public Optional<Boolean> getBypassArtifactCache() {
        return Optional.ofNullable(bypassArtifactCache);
    }

    @Override
    public YPath withBypassArtifactCache(boolean bypassArtifactCache) {
        RichYPath copy = new RichYPath(this);
        copy.bypassArtifactCache = bypassArtifactCache;
        return copy;
    }

    @Override
    public String toString() {
        return toStringImpl(false);
    }

    @Override
    public String toStableString() {
        return toStringImpl(true);
    }

    private String toStringImpl(boolean stable) {
        String simpleString = Stream.concat(Stream.of(rootDesignator), relativePath.stream())
                .collect(Collectors.joining("/"));
        YTreeNode node = toTree();
        if (node.containsAttributes()) {
            String tmp;
            if (stable) {
                tmp = YTreeTextSerializer.stableSerialize(buildAttributes(YTree.builder()).value(31337).build());
            } else {
                tmp = YTreeTextSerializer.serialize(buildAttributes(YTree.builder()).value(31337).build());
            }
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
                .when(append != null, b -> b.key("append").value(append))
                .when(primary != null, b -> b.key("primary").value(primary))
                .when(!ranges.isEmpty(), b -> b.key("ranges").value(ranges, (b2, r) -> {
                    YTreeBuilder rangesBuilder = b2.beginMap();
                    rangesBuilder = r.addRangeCriteria(rangesBuilder);
                    return rangesBuilder.endMap();
                }))
                .when(foreign != null, b -> b.key("foreign").value(foreign))
                .when(!columns.isEmpty(), b -> b.key("columns").value(columns))
                .when(!renameColumns.isEmpty(), b -> {
                    YTreeBuilder mapBuilder = YTree.mapBuilder();
                    for (Map.Entry<String, String> oldToNewColumn : renameColumns.entrySet()) {
                        mapBuilder.key(oldToNewColumn.getKey()).value(oldToNewColumn.getValue());
                    }
                    return b.key("rename_columns").value(mapBuilder.buildMap());
                })
                .when(!sortedBy.isEmpty(), b -> b.key("sorted_by").value(sortedBy))
                .when(timestamp != null, b -> b.key("timestamp").value(timestamp))
                .when(schema != null, b -> b.key("schema").value(schema))
                .when(format != null, b -> b.key("format").value(format))
                .when(bypassArtifactCache != null,
                        b -> b.key("bypass_artifact_cache").value(bypassArtifactCache))
                .when(executable != null, b -> b.key("executable").value(executable))
                .when(create != null, b -> b.key("create").value(create))
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
        return new RichYPath("/", List.of());
    }

    public static YPath objectRoot(GUID id) {
        return new RichYPath("#" + id.toString(), List.of());
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

    public static YPath fromString(String data) {
        return RichYPathParser.parse(data);
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
            return List.of();
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
        return List.copyOf(result);
    }

    static YPath fromAttributes(YPath simplePath, Map<String, YTreeNode> attrs) {
        Map<String, YTreeNode> attributes = new HashMap<>(attrs);

        Optional<Boolean> append = Optional.ofNullable(attributes.remove("append")).map(YTreeNode::boolValue);
        Optional<Boolean> primary = Optional.ofNullable(attributes.remove("primary")).map(YTreeNode::boolValue);
        Optional<Boolean> foreign = Optional.ofNullable(attributes.remove("foreign")).map(YTreeNode::boolValue);
        Optional<Boolean> bypassArtifactCache = Optional.ofNullable(attributes.remove("bypass_artifact_cache"))
                .map(YTreeNode::boolValue);
        Optional<Boolean> executable = Optional.ofNullable(attributes.remove("executable")).map(YTreeNode::boolValue);
        Optional<Boolean> create = Optional.ofNullable(attributes.remove("create")).map(YTreeNode::boolValue);

        List<RangeCriteria> ranges = new ArrayList<>();
        Optional<YTreeNode> rangesAttribute = Optional.ofNullable(attributes.remove("ranges"));
        if (rangesAttribute.isPresent()) {
            for (YTreeNode range : rangesAttribute.get().listNode()) {
                RangeLimit lower = getLimit(range, "lower_limit");
                RangeLimit upper = getLimit(range, "upper_limit");
                RangeLimit exact = getLimit(range, "exact");

                if (exact != null) {
                    ranges.add(new Exact(exact));
                } else {
                    ranges.add(Range.builder().setLowerLimit(lower).setUpperLimit(upper).build());
                }
            }
        }
        Set<String> columns = Optional.ofNullable(attributes.remove("columns"))
                .map(v -> v.asList().stream()).orElse(Stream.of())
                .map(YTreeNode::stringValue)
                .collect(Collectors.toSet());
        Map<String, String> renameColumns = Optional.ofNullable(attributes.remove("rename_columns"))
                .map(v -> v.asMap().entrySet().stream()).orElse(Stream.of())
                .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().stringValue()));
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
        if (create.isPresent()) {
            result = result.create(create.get());
        }

        if (!attributes.isEmpty()) {
            result = result.withAdditionalAttributes(attributes);
        }
        return result;
    }

    @Nullable
    private static RangeLimit getLimit(YTreeNode node, String key) {
        final YTreeMapNode parentMapNode = node.mapNode();
        if (!parentMapNode.containsKey(key)) {
            return null;
        }
        return RangeLimit.fromTree(parentMapNode.getOrThrow(key));
    }
}

enum TokenType {
    Literal,
    Slash,
    Ampersand,
    At,
    Asterisk,
    StartOfStream,
    EndOfStream,
    Range;

    public static TokenType fromByte(byte sym) {
        switch (sym) {
            case '/':
                return Slash;
            case '@':
                return At;
            case '&':
                return Ampersand;
            case '*':
                return Asterisk;
            default: {
                throw new RuntimeException(String.format("Impossible to convert '%s' to TokenType", sym));
            }
        }
    }
}

@NonNullApi
@NonNullFields
class YPathTokenizer {
    private final byte[] input;
    private TokenType type = TokenType.StartOfStream;
    private TokenType previousType = TokenType.StartOfStream;
    private int tokenLength = 0;
    private int inputShift = 0;
    private ByteArrayOutputStream literalValue = new ByteArrayOutputStream();

    YPathTokenizer(byte[] path) {
        this.input = path;
    }

    TokenType advance() {
        this.inputShift += tokenLength;
        literalValue = new ByteArrayOutputStream();
        if (inputShift == input.length) {
            setType(TokenType.EndOfStream);
            tokenLength = 0;
            return type;
        }

        setType(TokenType.Literal);
        boolean proceed = true;

        int currentIndex = 0;
        while (proceed && inputShift + currentIndex < input.length) {
            byte currentSymbol = input[inputShift + currentIndex];
            var currentToken = YsonTokenType.fromSymbol(currentSymbol);
            if (currentToken == RichYPathTags.BEGIN_COLUMN_SELECTOR_TOKEN
                    || currentToken == RichYPathTags.BEGIN_ROW_SELECTOR_TOKEN) {
                if (currentIndex == 0) {
                    setType(TokenType.Range);
                    currentIndex = input.length - inputShift;
                }
                proceed = false;
                continue;
            }

            switch (currentSymbol) {
                case '/':
                case '@':
                case '&':
                case '*': {
                    if (currentIndex == 0) {
                        this.tokenLength = 1;
                        setType(TokenType.fromByte(currentSymbol));
                        return type;
                    }
                    proceed = false;
                    break;
                }
                case '\\': {
                    currentIndex = advanceEscaped(currentIndex);
                    break;
                }
                default: {
                    literalValue.write(input[inputShift + currentIndex]);
                    ++currentIndex;
                    break;
                }
            }
        }
        tokenLength = currentIndex;
        return type;
    }

    private int advanceEscaped(int currentIndex) {
        if (input[inputShift + currentIndex] != '\\') {
            throw new IllegalStateException("'\\' was expected in advanceEscaped");
        }
        ++currentIndex;
        if (inputShift + currentIndex == input.length) {
            throw new IllegalArgumentException("Unexpected end-of-string in YPath while parsing escape sequence");
        }
        if (isSpecialCharacter(input[inputShift + currentIndex])) {
            literalValue.write(input[inputShift + currentIndex]);
            ++currentIndex;
        } else {
            if (input[inputShift + currentIndex] == 'x') {
                if (currentIndex + 2 >= input.length) {
                    throwMalformedEscapeSequence(ByteBuffer.wrap(input, inputShift + currentIndex - 1, input.length));
                }
                ByteBuffer context = ByteBuffer.wrap(
                        input, inputShift + currentIndex - 1, inputShift + currentIndex + 3);
                int hi = parseHexDigit(input[inputShift + currentIndex + 1], context);
                int lo = parseHexDigit(input[inputShift + currentIndex + 2], context);
                literalValue.write((hi << 4) + lo);
                currentIndex += 3;
            } else {
                throwMalformedEscapeSequence(ByteBuffer.wrap(
                        input, inputShift + currentIndex - 1, inputShift + currentIndex + 1));
            }
        }
        return currentIndex;
    }

    private void throwMalformedEscapeSequence(ByteBuffer context) {
        throw new RuntimeException(String.format("Malformed escape sequence %s in YPath", context));
    }

    private int parseHexDigit(byte ch, ByteBuffer context) {
        if (ch >= '0' && ch <= '9') {
            return ch - '0';
        }

        if (ch >= 'a' && ch <= 'f') {
            return ch - 'a' + 10;
        }

        if (ch >= 'A' && ch <= 'F') {
            return ch - 'A' + 10;
        }

        throwMalformedEscapeSequence(context);
        return 0;
    }

    private boolean isSpecialCharacter(byte ch) {
        return ch == '\\' || ch == '/' || ch == '@' || ch == '*' || ch == '&' || ch == '[' || ch == '{';
    }

    void setType(TokenType type) {
        this.previousType = this.type;
        this.type = type;
    }

    TokenType getType() {
        return type;
    }

    @Nullable
    byte[] getToken() {
        return tokenLength > 0 ? Arrays.copyOfRange(input, inputShift, inputShift + tokenLength) : null;
    }

    byte[] getPrefix() {
        return Arrays.copyOfRange(input, 0, inputShift);
    }
}
