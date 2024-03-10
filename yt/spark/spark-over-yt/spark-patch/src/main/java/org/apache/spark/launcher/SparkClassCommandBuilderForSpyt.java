package org.apache.spark.launcher;

import tech.ytsaurus.spyt.patch.annotations.OriginClass;
import tech.ytsaurus.spyt.patch.annotations.Subclass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Subclass
@OriginClass("org.apache.spark.launcher.SparkClassCommandBuilder")
public class SparkClassCommandBuilderForSpyt extends SparkClassCommandBuilder {

    private static final Map<String, String> DAEMON_SUBSTITUTES = Map.of(
            "org.apache.spark.deploy.master.YtMaster", "org.apache.spark.deploy.master.Master",
            "org.apache.spark.deploy.worker.YtWorker", "org.apache.spark.deploy.worker.Worker",
            "org.apache.spark.deploy.history.YtHistoryServer", "org.apache.spark.deploy.history.HistoryServer"
    );

    private static final Map<String, String> DAEMON_SUBSTITUTES_BACK =
            DAEMON_SUBSTITUTES.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    SparkClassCommandBuilderForSpyt(String className, List<String> classArgs) {
        super(DAEMON_SUBSTITUTES.getOrDefault(className, className), classArgs);
    }

    @Override
    public List<String> buildCommand(Map<String, String> env) throws IOException, IllegalArgumentException {
        List<String> cmd = super.buildCommand(env);
        for (int i = 0; i < cmd.size(); i++) {
            if (DAEMON_SUBSTITUTES_BACK.containsKey(cmd.get(i))) {
                cmd.set(i, DAEMON_SUBSTITUTES_BACK.get(cmd.get(i)));
            }
        }
        return cmd;
    }
}
