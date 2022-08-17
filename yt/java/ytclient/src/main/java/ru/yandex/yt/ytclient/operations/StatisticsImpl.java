package ru.yandex.yt.ytclient.operations;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;

public class StatisticsImpl implements Statistics {

    private static final int STATISTICS_FILE_DESCRIPTOR_NUMBER = 5;
    @Nullable
    private Yield<YTreeMapNode> yield = null;
    private long jobStartTime = 0;
    private String jobName;

    @Override
    public void start(String jobName) {
        jobStartTime = System.currentTimeMillis();
        this.jobName = jobName;
    }

    @Override
    public void finish() {
        if (jobName != null) {
            long delta = System.currentTimeMillis() - jobStartTime;

            write(YTree.builder()
                    .beginMap()
                    .key(jobName)
                    .beginMap()
                    .key("total_time").value(delta)
                    .endMap()
                    .endMap()
                    .build().mapNode());
        }
    }

    @Override
    public void write(YTreeMapNode metricsDict) {
        if (yield == null) {
            try {
                OutputStream output =
                        new BufferedOutputStream(YtUtils.outputStreamById(STATISTICS_FILE_DESCRIPTOR_NUMBER));
                yield = YTableEntryTypes.YSON.yield(new OutputStream[]{output});
            } catch (FileNotFoundException e) {
                yield = null;
            }
        }
        if (yield != null) {
            yield.yield(metricsDict);
        }
    }

    @Override
    public void close() throws IOException {
        if (yield != null) {
            yield.close();
        }
    }

}
