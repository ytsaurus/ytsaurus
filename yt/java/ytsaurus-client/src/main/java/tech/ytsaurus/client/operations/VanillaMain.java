package tech.ytsaurus.client.operations;

import java.io.OutputStream;


import tech.ytsaurus.core.operations.Yield;
public class VanillaMain {
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
        Yield<TOutput> yield = outputType.yield(output);
        try {
            return job.run(yield, statistics);
        } finally {
            yield.close();
            statistics.close();
        }
    }
}
