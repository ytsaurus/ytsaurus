package tech.ytsaurus.client.operations;

import java.io.OutputStream;

import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;

@NonNullApi
class VanillaMain {
    private VanillaMain() {
    }

    public static void main(String[] args) throws Exception {
        YtMainUtils.setTempDir();
        YtMainUtils.disableSystemOutput();
        OutputStream[] output = YtMainUtils.buildOutputStreams(args);

        VanillaJob mapper = (VanillaJob) YtMainUtils.construct(args);

        try {
            System.exit(apply(mapper, output, new StatisticsImpl()));
        } catch (Throwable e) {
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }

    public static <TOutput> int apply(VanillaJob<TOutput> job, OutputStream[] output, Statistics statistics)
            throws java.io.IOException {
        YTableEntryType<TOutput> outputType = job.outputType();
        try (statistics; Yield<TOutput> yield = outputType.yield(output)) {
            return job.run(yield, statistics);
        }
    }
}
