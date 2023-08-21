package tech.ytsaurus.client.operations;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class MapMain {
    private MapMain() {
    }

    public static void main(String[] args) throws Exception {
        YtMainUtils.setTempDir();
        YtMainUtils.disableSystemOutput();
        OutputStream[] output = YtMainUtils.buildOutputStreams(args);

        Mapper mapper = (Mapper) YtMainUtils.construct(args);

        try {
            applyMapper(mapper, System.in, output, new StatisticsImpl());
        } catch (Throwable e) {
            e.printStackTrace(System.err);
            System.exit(2);
        } finally {
            System.exit(0);
        }
    }

    public static <I, O> void applyMapper(Mapper<I, O> mapper,
                                          InputStream in, OutputStream[] output,
                                          Statistics statistics) throws Exception {
        long startTime = System.currentTimeMillis();
        YTableEntryType<I> inputType = mapper.inputType();
        YTableEntryType<O> outputType = mapper.outputType();

        try (statistics; Yield<O> yield = outputType.yield(output)) {
            OperationContext context = new OperationContext();
            Iterator<I> it = inputType.iterator(in, context);
            mapper.start(yield, statistics);
            mapper.map(it, yield, statistics, context);
            mapper.finish(yield, statistics);
        }

        System.err.printf("Total Time: %d\n", System.currentTimeMillis() - startTime);
    }
}
