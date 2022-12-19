package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TPrerequisiteOptions;
import tech.ytsaurus.ysontree.YTreeBuilder;

public class PrerequisiteOptions {
    public static class RevisionPrerequsite {
        private final String path;
        private final long revision;

        public RevisionPrerequsite(String path, long revision) {
            this.path = path;
            this.revision = revision;
        }
    }

    @Nullable
    private List<GUID> transactionsIds;
    @Nullable
    private List<RevisionPrerequsite> revisions;

    public PrerequisiteOptions() {
    }

    public PrerequisiteOptions(PrerequisiteOptions prerequisiteOptions) {
        transactionsIds = prerequisiteOptions.transactionsIds;
        revisions = prerequisiteOptions.revisions;
    }

    public PrerequisiteOptions setTransactionsIds(List<GUID> transactionIds) {
        this.transactionsIds = new ArrayList<>(transactionIds);
        return this;
    }

    public PrerequisiteOptions setTransactionsIds(GUID... transactionsIds) {
        this.transactionsIds = Arrays.asList(transactionsIds);
        return this;
    }

    public PrerequisiteOptions setRevisions(RevisionPrerequsite... revisions) {
        this.revisions = Arrays.asList(revisions);
        return this;
    }

    public YTreeBuilder toTree(YTreeBuilder builder) {
        if (transactionsIds != null && !transactionsIds.isEmpty()) {
            builder.key("prerequisite_transaction_ids").value(
                    transactionsIds.stream()
                            .map(GUID::toString)
                            .collect(Collectors.toList())
            );
        }
        if (revisions != null && !revisions.isEmpty()) {
            throw new IllegalArgumentException("revisions prerequisites are not supported yet");
        }

        return builder;
    }

    public TPrerequisiteOptions.Builder writeTo(TPrerequisiteOptions.Builder builder) {
        if (transactionsIds != null) {
            for (GUID guid : transactionsIds) {
                builder.addTransactions(TPrerequisiteOptions.TTransactionPrerequisite.newBuilder()
                        .setTransactionId(RpcUtil.toProto(guid)).build());
            }
        }
        if (revisions != null) {
            for (RevisionPrerequsite rev : revisions) {
                builder.addRevisions(TPrerequisiteOptions.TRevisionPrerequisite.newBuilder()
                        .setPath(rev.path)
                        .setRevision(rev.revision)
                        .build());
            }
        }
        return builder;
    }
}
