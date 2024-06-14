package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.rpcproxy.EContentType;
import tech.ytsaurus.rpcproxy.TReqStartQuery;

/**
 * Immutable query file.
 *
 * @see StartQuery.Builder#setQueryFiles(List)
 */
public class QueryFile {
    private final String name;
    private final String content;
    private final ContentType type;

    private QueryFile(Builder builder) {
        this.name = Objects.requireNonNull(builder.name);
        this.content = Objects.requireNonNull(builder.content);
        this.type = Objects.requireNonNull(builder.type);
    }

    public TReqStartQuery.TQueryFile toProto() {
        TReqStartQuery.TQueryFile.Builder builder = TReqStartQuery.TQueryFile.newBuilder();
        builder.setName(name);
        builder.setContent(content);
        builder.setType(type.getProtoValue());
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
        private String name;
        @Nullable
        private String content;
        @Nullable
        private ContentType type;

        private Builder() {
        }

        /**
         * Set file name.
         *
         * @return self
         */
        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Set file content.
         *
         * @return self
         */
        public Builder setContent(String content) {
            this.content = content;
            return this;
        }

        /**
         * Set file content type.
         *
         * @return self
         */
        public Builder setContentType(ContentType contentType) {
            this.type = contentType;
            return this;
        }

        /**
         * Construct {@link RowBatchReadOptions} instance.
         */
        public QueryFile build() {
            return new QueryFile(this);
        }
    }

    public enum ContentType {
        RawInlineData(EContentType.CT_RAW_INLINE_DATA, "raw_inline_data"),
        Url(EContentType.CT_URL, "url");

        private final EContentType protoValue;
        private final String stringValue;

        ContentType(EContentType protoValue, String stringValue) {
            this.protoValue = protoValue;
            this.stringValue = stringValue;
        }

        @Override
        public String toString() {
            return stringValue;
        }

        EContentType getProtoValue() {
            return protoValue;
        }
    }
}
