package tech.ytsaurus.core.cypress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;


/**
 * @author sankear
 */
@NonNullApi
@NonNullFields
public class RangeLimit {
    @SuppressWarnings("VisibilityModifier")
    public final List<YTreeNode> key;
    @Nullable
    private final KeyBound keyBound;
    @SuppressWarnings("VisibilityModifier")
    public final long rowIndex;
    @SuppressWarnings("VisibilityModifier")
    public final long offset;
    @SuppressWarnings("VisibilityModifier")
    public final long tabletIndex;

    RangeLimit(List<YTreeNode> key, @Nullable KeyBound keyBound, long rowIndex, long offset) {
        this(key, keyBound, rowIndex, offset, -1);
    }

    RangeLimit(List<YTreeNode> key, @Nullable KeyBound keyBound, long rowIndex, long offset, long tabletIndex) {
        this.key = key;
        this.keyBound = keyBound;
        this.rowIndex = rowIndex;
        this.offset = offset;
        this.tabletIndex = tabletIndex;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RangeLimit that = (RangeLimit) o;
        return rowIndex == that.rowIndex
                && offset == that.offset
                && tabletIndex == that.tabletIndex
                && Objects.equals(key, that.key)
                && Objects.equals(keyBound, that.keyBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, keyBound, rowIndex, offset, tabletIndex);
    }

    public static RangeLimit row(long rowIndex) {
        return new RangeLimit(Collections.emptyList(), null, rowIndex, -1);
    }

    public static RangeLimit offset(long offset) {
        return new RangeLimit(Collections.emptyList(), null, -1, offset);
    }

    public static RangeLimit key(List<YTreeNode> key) {
        return new RangeLimit(key, null, -1, -1);
    }

    public static RangeLimit key(YTreeNode... key) {
        return key(Arrays.asList(key));
    }

    public static RangeLimit key(Relation relation, YTreeNode... key) {
        return new RangeLimit(Collections.emptyList(), KeyBound.of(relation, key), -1, -1);
    }

    public static RangeLimit fromTree(YTreeNode node) {
        YTreeMapNode mapNode = node.mapNode();
        List<YTreeNode> limitKey = new ArrayList<>();
        if (mapNode.containsKey("key")) {
            limitKey = mapNode.getOrThrow("key").asList();
        }
        KeyBound keyBound = null;
        if (mapNode.containsKey("key_bound")) {
            keyBound = KeyBound.fromTree(mapNode.getOrThrow("key_bound"));
        }
        long rowIndex = -1;
        if (mapNode.containsKey("row_index")) {
            rowIndex = mapNode.getOrThrow("row_index").longValue();
        }
        long offset = -1;
        if (mapNode.containsKey("offset")) {
            offset = mapNode.getOrThrow("offset").longValue();
        }
        long tabletIndex = -1;
        if (mapNode.containsKey("tablet_index")) {
            tabletIndex = mapNode.getOrThrow("tablet_index").longValue();
        }
        return RangeLimit.builder()
                .setKey(limitKey)
                .setKeyBound(keyBound)
                .setRowIndex(rowIndex)
                .setOffset(offset)
                .setTabletIndex(tabletIndex)
                .build();
    }

    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder.beginMap()
                .when(!key.isEmpty(), b -> b.key("key").value(key))
                .when(keyBound != null, b -> {
                    assert keyBound != null;
                    return b.key("key_bound").apply(keyBound::toTree);
                })
                .when(rowIndex != -1, b -> b.key("row_index").value(rowIndex))
                .when(offset != -1, b -> b.key("offset").value(offset))
                .when(tabletIndex != -1, b -> b.key("tablet_index").value(tabletIndex))
                .endMap();
    }

    public Builder toBuilder() {
        return builder()
                .setKeyBound(keyBound)
                .setKey(key)
                .setRowIndex(rowIndex)
                .setOffset(offset)
                .setTabletIndex(tabletIndex);
    }

    @NonNullApi
    @NonNullFields
    public static class Builder {
        private List<YTreeNode> key = Collections.emptyList();
        @Nullable
        private KeyBound keyBound = null;
        private long rowIndex = -1;
        private long offset = -1;
        private long tabletIndex = -1;

        public Builder() {
        }

        public Builder setKey(YTreeNode... key) {
            return setKey(Arrays.asList(key));
        }

        public Builder setKey(List<YTreeNode> key) {
            this.key = key;
            return this;
        }

        public Builder setKeyBound(@Nullable KeyBound keyBound) {
            this.keyBound = keyBound;
            return this;
        }

        public Builder setRowIndex(long rowIndex) {
            this.rowIndex = rowIndex;
            return this;
        }

        public Builder setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder setTabletIndex(long tabletIndex) {
            this.tabletIndex = tabletIndex;
            return this;
        }

        public RangeLimit build() {
            return new RangeLimit(new ArrayList<>(key), keyBound, rowIndex, offset, tabletIndex);
        }
    }
}
