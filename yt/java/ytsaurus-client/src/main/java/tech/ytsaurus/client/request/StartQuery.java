package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TReqStartQuery;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Immutable start query request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#startQuery(StartQuery)
 */
public class StartQuery extends QueryTrackerReq<StartQuery.Builder, StartQuery>
        implements HighLevelRequest<TReqStartQuery.Builder> {
    private final QueryEngine queryEngine;
    private final String query;
    @Nullable
    private final YTreeNode settings;
    private final boolean draft;
    @Nullable
    private final YTreeMapNode annotations;
    private final List<QueryFile> files;
    @Nullable
    private final List<String> accessControlObjects;
    private final List<QuerySecret> secrets;

    StartQuery(Builder builder) {
        super(builder);
        this.queryEngine = Objects.requireNonNull(builder.queryEngine);
        this.query = Objects.requireNonNull(builder.query);
        this.settings = builder.settings;
        this.draft = builder.draft;
        this.annotations = builder.annotations;
        this.files = Objects.requireNonNull(builder.files);
        this.accessControlObjects = builder.accessControlObjects;
        this.secrets = Objects.requireNonNull(builder.secrets);
    }

    /**
     * Construct empty builder for start query request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        return builder()
                .setQueryEngine(queryEngine)
                .setQuery(query)
                .setSettings(settings)
                .setDraft(draft)
                .setAnnotations(annotations)
                .setQueryFiles(files)
                .setAccessControlObjects(accessControlObjects)
                .setQueryTrackerStage(queryTrackerStage)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData)
                .setQuerySecrets(secrets);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqStartQuery.Builder, ?> requestBuilder) {
        TReqStartQuery.Builder builder = requestBuilder.body();
        writeQueryTrackerDescriptionToProto(builder::setQueryTrackerStage);
        builder.setEngine(queryEngine.getProtoValue());
        builder.setQuery(query);
        builder.setDraft(draft);
        if (settings != null) {
            builder.setSettings(ByteString.copyFrom(settings.toBinary()));
        }
        if (annotations != null) {
            builder.setAnnotations(ByteString.copyFrom(annotations.toBinary()));
        }
        if (accessControlObjects != null) {
            builder.setAccessControlObjects(
                    TReqStartQuery.TAccessControlObjects.newBuilder()
                            .addAllItems(accessControlObjects)
                            .build()
            );
        }
        builder.addAllFiles(files.stream().map(QueryFile::toProto).collect(Collectors.toList()));
        builder.addAllSecrets(secrets.stream().map(QuerySecret::toProto).collect(Collectors.toList()));
    }

    /**
     * Builder for {@link StartQuery}
     */
    public static class Builder extends QueryTrackerReq.Builder<StartQuery.Builder, StartQuery> {
        @Nullable
        private QueryEngine queryEngine;
        @Nullable
        private String query;
        @Nullable
        private YTreeNode settings;
        private boolean draft;
        @Nullable
        private YTreeMapNode annotations;
        @Nullable
        private List<QueryFile> files;
        @Nullable
        private List<String> accessControlObjects;
        @Nullable
        private List<QuerySecret> secrets;

        private Builder() {
        }

        /**
         * Set query engine.
         *
         * @return self
         * @see QueryEngine
         */
        public Builder setQueryEngine(QueryEngine queryEngine) {
            this.queryEngine = queryEngine;
            return self();
        }

        /**
         * Set query text.
         *
         * @return self
         */
        public Builder setQuery(String query) {
            this.query = query;
            return self();
        }

        /**
         * Set additional query parameters.
         *
         * @return self
         */
        public Builder setSettings(@Nullable YTreeNode settings) {
            this.settings = settings;
            return self();
        }

        /**
         * Set whether the query is a draft.
         * Such queries complete automatically without execution.
         *
         * @return self
         */
        public Builder setDraft(boolean draft) {
            this.draft = draft;
            return self();
        }


        /**
         * Set custom annotations for the query.
         * Allows to conveniently search for queries.
         *
         * @return self
         */
        public Builder setAnnotations(@Nullable YTreeMapNode annotations) {
            this.annotations = annotations;
            return self();
        }


        /**
         * Set list of files for query.
         *
         * @return self
         * @see QueryFile
         */
        public Builder setQueryFiles(List<QueryFile> files) {
            this.files = files;
            return self();
        }

        /**
         * Set names of objects in "//sys/access_control_object_namespaces/queries/",
         * which determine access to the query for other users.
         *
         * @return self
         */
        public Builder setAccessControlObjects(@Nullable List<String> accessControlObjects) {
            this.accessControlObjects = accessControlObjects;
            return self();
        }

        /**
         * Set list of secrets for query.
         *
         * @return self
         * @see QuerySecret
         */
        public Builder setQuerySecrets(List<QuerySecret> secrets) {
            this.secrets = secrets;
            return self();
        }

        /**
         * Construct {@link StartQuery} instance.
         */
        public StartQuery build() {
            return new StartQuery(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
