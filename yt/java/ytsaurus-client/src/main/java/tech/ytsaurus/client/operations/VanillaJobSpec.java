package tech.ytsaurus.client.operations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;


/**
 * Immutable vanilla job spec.
 */
@NonNullApi
@NonNullFields
public class VanillaJobSpec extends MapperOrReducerSpec implements Spec {
    private final List<YPath> outputTablePaths;

    protected VanillaJobSpec(Builder builder) {
        super(VanillaMain.class, builder);
        outputTablePaths = builder.outputTablePaths;
    }

    /**
     * Convert to yson.
     */
    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt,
                                SpecPreparationContext specPreparationContext,
                                FormatContext formatContext) {
        return prepare(builder, yt, specPreparationContext);
    }

    /**
     * Convert to yson.
     */
    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt,
                                SpecPreparationContext specPreparationContext) {
        if (outputTablePaths.isEmpty()) {
            var formatContext = FormatContext.builder()
                    .setOutputTableCount(0)
                    .build();
            return super.prepare(builder, yt, specPreparationContext, formatContext);
        } else {
            var formatContext = FormatContext.builder()
                    .setOutputTableCount(outputTablePaths.size())
                    .build();
            YTreeBuilder prepare = super.prepare(YTree.builder(), yt, specPreparationContext, formatContext);
            Map<String, YTreeNode> node = new HashMap<>(prepare.build().asMap());

            node.put("output_table_paths", YTree.builder()
                    .value(outputTablePaths.stream().map(YPath::toTree).collect(Collectors.toList()))
                    .build());
            return builder.value(node);
        }
    }

    /**
     * Create spec builder from vanilla job.
     */
    public static Builder builder(VanillaJob<?> job) {
        Builder builder = new Builder();
        builder.setJob(job);
        return builder;
    }

    /**
     * Create empty builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder of {@link VanillaJobSpec}.
     */
    @NonNullApi
    @NonNullFields
    public static class Builder extends MapperOrReducerSpec.Builder<Builder> {
        private List<YPath> outputTablePaths = List.of();

        Builder() {
            // Vanilla jobs require setting job count
            setJobCount(1);
        }

        @Override
        public VanillaJobSpec build() {
            if (getUserJob() == null) {
                throw new IllegalStateException("Job is required and has no default value");
            }
            return new VanillaJobSpec(this);
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Set user job, it is required parameter.
         */
        public Builder setJob(VanillaJob<?> job) {
            setUserJob(job);
            return this;
        }

        /**
         * Sets job_count.
         * <p>
         * This setter overridden from our base class performs additional checks.
         * Vanilla job_count must always be set and its value must be greater or equal to 0.
         */
        @Override
        public Builder setJobCount(Integer jobCount) {
            if (jobCount < 1) {
                throw new IllegalArgumentException("Vanilla's job_count must be >= 1");
            }
            return super.setJobCount(jobCount);
        }

        /**
         * Set output tables.
         */
        public Builder setOutputTablePaths(List<YPath> outputTablePaths) {
            this.outputTablePaths = outputTablePaths;
            return this;
        }
    }
}
