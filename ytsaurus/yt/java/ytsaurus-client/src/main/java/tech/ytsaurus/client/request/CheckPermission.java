package tech.ytsaurus.client.request;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TMasterReadOptions;
import tech.ytsaurus.rpcproxy.TPrerequisiteOptions;
import tech.ytsaurus.rpcproxy.TReqCheckPermission;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;

public class CheckPermission
        extends MutateNode<CheckPermission.Builder, CheckPermission>
        implements HighLevelRequest<TReqCheckPermission.Builder> {
    private final String user;
    private final String path;
    private final int permissions;
    private final Set<String> columns;
    @Nullable
    private final MasterReadOptions masterReadOptions;

    CheckPermission(Builder builder) {
        super(builder);
        this.user = Objects.requireNonNull(builder.user);
        this.path = Objects.requireNonNull(builder.path);
        this.permissions = Objects.requireNonNull(builder.permissions);
        this.columns = Objects.requireNonNull(builder.columns);
        this.masterReadOptions = builder.masterReadOptions;
    }

    public CheckPermission(String user, String path, int permissions) {
        this(user, path, permissions, Collections.emptySet());
    }

    public CheckPermission(String user, String path, int permissions, Set<String> columns) {
        this(builder().setUser(user).setPath(path).setPermissions(permissions).setColumns(columns));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCheckPermission.Builder, ?> requestBuilder) {
        TReqCheckPermission.Builder builder = requestBuilder.body();

        builder
                .setUser(user)
                .setPath(path)
                .setPermission(permissions)
                .setColumns(TReqCheckPermission.TColumns.newBuilder().addAllItems(columns));

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        if (masterReadOptions != null) {
            builder.setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("Path: ").append(path).append("; ");
        sb.append("User: ").append(user).append("; ");
        sb.append("Permissions: ").append(permissions).append("; ");
        super.writeArgumentsLogString(sb);
    }

    public Builder toBuilder() {
        Builder builder = builder()
                .setUser(user)
                .setPath(path)
                .setPermissions(permissions)
                .setColumns(columns)
                .setTransactionalOptions(transactionalOptions != null
                        ? new TransactionalOptions(transactionalOptions)
                        : null)
                .setPrerequisiteOptions(prerequisiteOptions != null
                        ? new PrerequisiteOptions(prerequisiteOptions)
                        : null)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);

        builder.setMutatingOptions(new MutatingOptions(mutatingOptions));
        if (masterReadOptions != null) {
            builder.setMasterReadOptions(new MasterReadOptions(masterReadOptions));
        }
        return builder;
    }

    public static class Builder extends MutateNode.Builder<Builder, CheckPermission> {
        @Nullable
        private String user;
        @Nullable
        private String path;
        @Nullable
        private Integer permissions;
        @Nullable
        private Set<String> columns = Collections.emptySet();
        @Nullable
        private MasterReadOptions masterReadOptions;

        public Builder setUser(String user) {
            this.user = user;
            return self();
        }

        public Builder setPath(String path) {
            this.path = path;
            return self();
        }

        public Builder setPermissions(int permissions) {
            this.permissions = permissions;
            return self();
        }

        public Builder setColumns(Set<String> columns) {
            this.columns = new HashSet<>(columns);
            return self();
        }

        public Builder setMasterReadOptions(MasterReadOptions masterReadOptions) {
            this.masterReadOptions = masterReadOptions;
            return self();
        }

        public CheckPermission build() {
            return new CheckPermission(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
