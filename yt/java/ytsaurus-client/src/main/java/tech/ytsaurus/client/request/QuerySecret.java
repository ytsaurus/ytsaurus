package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.rpcproxy.TReqStartQuery;

/**
 * Immutable query secret.
 *
 * @see StartQuery.Builder#setQuerySecrets(List)
 */
public class QuerySecret {
    private final String id;
    private final String category;
    private final String subcategory;
    private final String ypath;

    private QuerySecret(Builder builder) {
        this.id = Objects.requireNonNull(builder.id);
        this.category = Objects.requireNonNull(builder.category);
        this.subcategory = Objects.requireNonNull(builder.subcategory);
        this.ypath = Objects.requireNonNull(builder.ypath);
    }

    public TReqStartQuery.TSecret toProto() {
        TReqStartQuery.TSecret.Builder builder = TReqStartQuery.TSecret.newBuilder();
        builder.setId(id);
        builder.setCategory(category);
        builder.setSubcategory(subcategory);
        builder.setYpath(ypath);
        return builder.build();
    }

    /**
     * Construct empty builder for row batch read options.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        @Nullable
        private String id;
        @Nullable
        private String category;
        @Nullable
        private String subcategory;
        @Nullable
        private String ypath;

        private Builder() {
        }

        /**
         * Set secret id.
         *
         * @return self
         */
        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        /**
         * Set secret category.
         *
         * @return self
         */
        public Builder setCategory(String category) {
            this.category = category;
            return this;
        }

        /**
         * Set secret subcategory.
         *
         * @return self
         */
        public Builder setSubcategory(String subcategory) {
            this.subcategory = subcategory;
            return this;
        }

        /**
         * Set path to secret value in cypress.
         *
         * @return self
         */
        public Builder setYpath(String ypath) {
            this.ypath = ypath;
            return this;
        }

        /**
         * Construct {@link RowBatchReadOptions} instance.
         */
        public QuerySecret build() {
            return new QuerySecret(this);
        }
    }
}

