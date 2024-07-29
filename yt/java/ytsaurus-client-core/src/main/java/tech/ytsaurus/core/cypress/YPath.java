package tech.ytsaurus.core.cypress;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;


/**
 * @author sankear
 */
public interface YPath {
    YPath parent();

    /**
     * @return path without RichPath additional attributes
     */
    YPath justPath();

    String name();

    YPath child(String key);

    YPath after(int index);

    YPath before(int index);

    YPath begin();

    YPath end();

    YPath child(int index);

    YPath attribute(String key);

    YPath all();

    YPath allAttributes();

    boolean isRoot();

    boolean isAttribute();

    boolean hasObjectRootDesignator();

    YPath withObjectRoot(GUID id);

    Optional<YTreeNode> getSchema();

    YPath withSchema(YTreeNode schema);

    Optional<String> getFormat();

    /**
     * Пример использования:
     * - YPath.simple("//some/table").withFormat("yson")
     * Комментарий к примеру: удобно, например, для того, чтобы при запуске джобы указанная таблица-словарь
     * автоматически была доставлена в локальную рабочую директорию джобы как yson-файл.
     */
    YPath withFormat(String format);

    Optional<Boolean> getBypassArtifactCache();

    YPath withBypassArtifactCache(boolean bypassArtifactCache);

    Optional<Boolean> getAppend();

    YPath append(boolean append);

    Optional<Boolean> getPrimary();

    YPath primary(boolean primary);

    Optional<Boolean> getForeign();

    YPath foreign(boolean foreign);

    default YPath withRange(long lowerRowIndex, long upperRowIndex) {
        return withRange(RangeLimit.row(lowerRowIndex), RangeLimit.row(upperRowIndex));
    }

    default YPath withRange(RangeLimit lower, RangeLimit upper) {
        return plusRange(new Range(lower, upper));
    }

    default YPath withExact(RangeLimit exact) {
        return plusRange(new Exact(exact));
    }

    List<RangeCriteria> getRanges();

    YPath ranges(List<RangeCriteria> ranges);

    default YPath ranges(RangeCriteria... ranges) {
        return ranges(Arrays.asList(ranges));
    }

    YPath plusRange(RangeCriteria range);

    List<String> getColumns();

    YPath withColumns(Collection<String> columns);

    default YPath withColumns(String... columns) {
        return withColumns(Arrays.asList(columns));
    }

    Map<String, String> getRenameColumns();

    YPath withRenameColumns(Map<String, String> renameColumns);

    default YPath plusRenameColumns(String oldColumnName, String newColumnName) {
        Map<String, String> map = new HashMap<>();
        map.put(oldColumnName, newColumnName);
        return plusRenameColumns(map);
    }

    YPath plusRenameColumns(Map<String, String> renameColumns);

    List<String> getSortedBy();

    YPath sortedBy(List<String> sortedBy);

    default YPath sortedBy(String... sortedBy) {
        return sortedBy(Arrays.asList(sortedBy));
    }

    Optional<Long> getTimestamp();

    YPath withTimestamp(long timestamp);

    default Optional<YtTimestamp> getYtTimestamp() {
        return getTimestamp().map(YtTimestamp::valueOf);
    }

    default YPath withYtTimestamp(YtTimestamp ytTimestamp) {
        return withTimestamp(ytTimestamp.getValue());
    }

    Optional<Boolean> getExecutable();

    YPath withExecutable(boolean executable);

    Optional<Boolean> getCreate();

    YPath create(boolean create);

    Optional<YTreeNode> getAdditionalAttribute(String attributeName);

    Map<String, YTreeNode> getAdditionalAttributes();

    YPath withAdditionalAttributes(Map<String, YTreeNode> additionalAttributes);

    default YPath plusAdditionalAttribute(String key, Object value) {
        return plusAdditionalAttribute(key, YTree.node(value));
    }

    YPath plusAdditionalAttribute(String key, YTreeNode value);

    YTreeNode toTree();

    YTreeBuilder toTree(YTreeBuilder builder);

    String toStableString();

    static YPath cypressRoot() {
        return RichYPath.cypressRoot();
    }

    static YPath objectRoot(GUID id) {
        return RichYPath.objectRoot(id);
    }

    static YPath simple(String path) {
        return RichYPath.simple(path);
    }

    static YPath fromTree(YTreeNode node) {
        return RichYPath.fromTree(node);
    }
}
