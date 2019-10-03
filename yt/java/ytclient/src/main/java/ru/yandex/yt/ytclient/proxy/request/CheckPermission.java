package ru.yandex.yt.ytclient.proxy.request;

import java.util.Set;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqCheckPermission;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class CheckPermission extends MutateNode<CheckPermission> {
    private final String user;
    private final String path;
    private final int permissions;
    private final Set<String> columns;

    public CheckPermission(String user, String path, int permissions) {
        this(user, path, permissions, Cf.set());
    }

    public CheckPermission(String user, String path, int permissions, Set<String> columns) {
        this.user = user;
        this.path = path;
        this.permissions = permissions;
        this.columns = columns;
    }

    public TReqCheckPermission.Builder writeTo(TReqCheckPermission.Builder builder) {

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }

        return builder
                .setUser(user)
                .setPath(path)
                .setPermission(permissions)
                .setColumns(TReqCheckPermission.TColumns.newBuilder().addAllItems(columns).build());
    }
}
