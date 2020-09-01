package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

public class PrerequisiteOptions {
    public class RevisionPrerequsite {
        public final String path;
        public final long revision;

        public RevisionPrerequsite(String path, long revision) {
            this.path = path;
            this.revision = revision;
        }
    }

    private GUID [] transactionId;
    private RevisionPrerequsite [] revisions;

    public PrerequisiteOptions() {
    }

    public PrerequisiteOptions(PrerequisiteOptions prerequisiteOptions) {
        transactionId = prerequisiteOptions.transactionId;
        revisions = prerequisiteOptions.revisions;
    }

    public PrerequisiteOptions setTransactionId(GUID [] transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public PrerequisiteOptions setRevisions(RevisionPrerequsite [] revisions) {
        this.revisions = revisions;
        return this;
    }

    public TPrerequisiteOptions.Builder writeTo(TPrerequisiteOptions.Builder builder) {
        if (transactionId != null) {
            for (int i = 0; i < transactionId.length; ++i) {
                builder.addTransactions(TPrerequisiteOptions.TTransactionPrerequisite.newBuilder()
                        .setTransactionId(RpcUtil.toProto(transactionId[i])).build());
            }
        }
        if (revisions != null) {
            for (int i = 0; i < revisions.length; ++i) {
                RevisionPrerequsite rev = revisions[i];
                builder.addRevisions(TPrerequisiteOptions.TRevisionPrerequisite.newBuilder()
                        .setPath(rev.path)
                        .setRevision(rev.revision)
                        .build());
            }
        }
        return builder;
    }
}
