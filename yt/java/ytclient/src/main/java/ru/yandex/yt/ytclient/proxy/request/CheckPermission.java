package ru.yandex.yt.ytclient.proxy.request;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqCheckPermission;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class CheckPermission
        extends MutateNode<CheckPermission>
        implements HighLevelRequest<TReqCheckPermission.Builder> {
    private final String user;
    private final String path;
    private final int permissions;
    private final Set<String> columns;

    private MasterReadOptions masterReadOptions;

    public CheckPermission(String user, String path, int permissions) {
        this(user, path, permissions, Collections.emptySet());
    }

    public CheckPermission(String user, String path, int permissions, Set<String> columns) {
        this.user = user;
        this.path = path;
        this.permissions = permissions;
        this.columns = columns;
    }

    public CheckPermission setMasterReadOptions(MasterReadOptions masterReadOptions) {
        this.masterReadOptions = masterReadOptions;
        return this;
    }

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

    @Nonnull
    @Override
    protected CheckPermission self() {
        return this;
    }
}
