package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqLinkNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class LinkNode extends CopyLikeReq<LinkNode> {
    public LinkNode(String src, String dst) {
        super(src, dst);
    }

    public LinkNode(YPath src, YPath dst) {
        this(src.toString(), dst.toString());
    }

    public TReqLinkNode.Builder writeTo(TReqLinkNode.Builder builder) {
        builder.setSrcPath(source)
                .setDstPath(destination)
                .setRecursive(recursive)
                .setForce(force)
                .setIgnoreExisting(ignoreExisting);

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (mutatingOptions != null) {
            builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }

        return builder;
    }
}
