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

    RangeLimit(List<YTreeNode> key, @Nullable KeyBound keyBound, long rowIndex, long offset) {
        this.key = key;
        this.keyBound = keyBound;
        this.rowIndex = rowIndex;
        this.offset = offset;
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
                && Objects.equals(key, that.key)
                && Objects.equals(keyBound, that.keyBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, keyBound, rowIndex, offset);
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

    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder.beginMap()
                .when(!key.isEmpty(), b -> b.key("key").value(key))
                .when(keyBound != null, b -> {
                    assert keyBound != null;
                    return b.key("key_bound").apply(keyBound::toTree);
                })
                .when(rowIndex != -1, b -> b.key("row_index").value(rowIndex))
                .when(offset != -1, b -> b.key("offset").value(offset))
                .endMap();
    }

    public Builder toBuilder() {
        return builder()
                .setKeyBound(keyBound)
                .setKey(key)
                .setRowIndex(rowIndex)
                .setOffset(offset);
    }

    @NonNullApi
    @NonNullFields
    public static class Builder {
        private List<YTreeNode> key = Collections.emptyList();
        @Nullable
        private KeyBound keyBound = null;
        private long rowIndex = -1;
        private long offset = -1;

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

        public RangeLimit build() {
            return new RangeLimit(new ArrayList<>(key), keyBound, rowIndex, offset);
        }
    }
}
