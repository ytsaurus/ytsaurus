package tech.ytsaurus.client.operations;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;


/**
 * Immutable spec for remote copy operation.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/remote_copy">
 * remote_copy documentation
 * </a>
 */
@NonNullApi
@NonNullFields
public class RemoteCopySpec extends SystemOperationSpecBase implements Spec {
    private final String cluster;
    private final @Nullable
    String network;
    private final @Nullable
    Boolean copyAttributes;

    /**
     * Create remote copy spec for paths and cluster with other options set to defaults.
     */
    public RemoteCopySpec(YPath source, YPath destination, String cluster) {
        this(builder()
                .setInputTables(source)
                .setOutputTable(destination)
                .setCluster(cluster)
        );
    }

    protected <T extends BuilderBase<T>> RemoteCopySpec(BuilderBase<T> builder) {
        super(builder);
        this.cluster = Objects.requireNonNull(builder.cluster);
        this.network = builder.network;
        this.copyAttributes = builder.copyAttributes;

        Objects.requireNonNull(this.cluster);
    }

    /**
     * Construct empty builder for remote copy spec.
     */
    public static BuilderBase<?> builder() {
        return new Builder();
    }

    /**
     * Create output table if it does not exist and convert remote copy spec to yson.
     */
    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt,
                                SpecPreparationContext specPreparationContext) {
        SpecUtils.createOutputTables(
                yt,
                specPreparationContext.getTransactionalOptions().orElse(null),
                List.of(getOutputTable()),
                getOutputTableAttributes()
        );

        return builder.beginMap()
                .apply(b -> toTree(b, specPreparationContext))
                .key("cluster_name").value(cluster)
                .when(network != null, b -> b.key("network_name").value(network))
                .when(copyAttributes != null, b -> b.key("copy_attributes").value(copyAttributes))
                .endMap();
    }

    /**
     * Builder for {@link RemoteCopySpec}
     */
    protected static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    // BuilderBase was taken out because there is another client
    // which we need to support too and which use the same RemoteCopySpec class.
    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>> extends SystemOperationSpecBase.Builder<T> {
        private @Nullable
        String cluster;
        private @Nullable
        String network;
        private @Nullable
        Boolean copyAttributes;

        /**
         * Create instance of {@link RemoteCopySpec}.
         */
        public RemoteCopySpec build() {
            return new RemoteCopySpec(this);
        }

        /**
         * Set the name of the cluster to copy data from.
         */
        public T setCluster(String cluster) {
            this.cluster = cluster;
            return self();
        }

        public T setNetwork(@Nullable String network) {
            this.network = network;
            return self();
        }

        /**
         * Set whether to copy the attributes of the input table (only available if there is only one input table).
         * Only user attributes (not system ones) are copied.
         */
        public T setCopyAttributes(@Nullable Boolean copyAttributes) {
            this.copyAttributes = copyAttributes;
            return self();
        }
    }
}
