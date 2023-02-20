package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.client.rows.MappedRowSerializer;
import tech.ytsaurus.client.rows.WireProtocolWriter;
import tech.ytsaurus.client.rows.WireRowSerializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;

@NonNullApi
@NonNullFields
public class MappedLookupRowsRequest<T>
        extends AbstractLookupRowsRequest<MappedLookupRowsRequest.Builder<T>, MappedLookupRowsRequest<T>> {
    private final List<T> rows;
    private final WireRowSerializer<T> serializer;

    public MappedLookupRowsRequest(BuilderBase<T, ?> builder) {
        super(builder);
        this.rows = new ArrayList<>(builder.rows);
        this.serializer = Objects.requireNonNull(builder.serializer);
    }

    public MappedLookupRowsRequest(String path, YTreeObjectSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    public MappedLookupRowsRequest(String path, WireRowSerializer<T> serializer) {
        this(MappedLookupRowsRequest.<T>builder().setPath(path).setSerializer(serializer));
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public void convertValues(SerializationResolver serializationResolver) {
    }

    @Override
    public void serializeRowsetTo(List<byte[]> attachments) {
        // Записываем только ключи
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(rows, serializer, i -> true);
        writer.finish();
    }

    @Override
    public Builder<T> toBuilder() {
        return MappedLookupRowsRequest.<T>builder()
                .setSerializer(serializer)
                .addFilters(rows)
                .setPath(path)
                .setSchema(schema)
                .setTimestamp(timestamp)
                .setRetentionTimestamp(retentionTimestamp)
                .setKeepMissingRows(keepMissingRows)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder<T> extends BuilderBase<T, Builder<T>> {
        @Override
        protected Builder<T> self() {
            return this;
        }

        @Override
        public MappedLookupRowsRequest<T> build() {
            return new MappedLookupRowsRequest<T>(this);
        }
    }

    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<
            T,
            TBuilder extends BuilderBase<T, TBuilder>>
            extends AbstractLookupRowsRequest.Builder<TBuilder, MappedLookupRowsRequest<T>> {
        private final List<T> rows = new ArrayList<>();
        @Nullable
        private WireRowSerializer<T> serializer;
        @Nullable
        private TableSchema mappedSchema;

        public TBuilder setSerializer(YTreeObjectSerializer<T> serializer) {
            return setSerializer(MappedRowSerializer.forClass(serializer));
        }

        public TBuilder setSerializer(WireRowSerializer<T> serializer) {
            setSchema(serializer.getSchema().toLookup());
            this.serializer = serializer;
            this.mappedSchema = serializer.getSchema();
            return self();
        }

        public TBuilder addAllLookupColumns() {
            checkMappedSchema();
            int count = Objects.requireNonNull(mappedSchema).getColumnsCount();
            for (int i = 0; i < count; i++) {
                addLookupColumn(mappedSchema.getColumnName(i));
            }
            return self();
        }

        public TBuilder addKeyLookupColumns() {
            checkMappedSchema();
            int count = Objects.requireNonNull(mappedSchema).getKeyColumnsCount();
            for (int i = 0; i < count; i++) {
                addLookupColumn(mappedSchema.getColumnName(i));
            }
            return self();
        }

        public TBuilder addFilter(T filter) {
            rows.add(filter);
            return self();
        }

        public TBuilder addFilters(Iterable<? extends T> filters) {
            for (T filter : filters) {
                addFilter(filter);
            }
            return self();
        }

        private void checkMappedSchema() {
            if (mappedSchema == null) {
                throw new IllegalStateException("serializer should be set before");
            }
        }
    }
}
