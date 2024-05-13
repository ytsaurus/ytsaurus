package org.apache.spark.launcher;

import tech.ytsaurus.spyt.patch.annotations.OriginClass;
import tech.ytsaurus.spyt.patch.annotations.Subclass;

import java.io.IOException;
import java.util.List;

import static org.apache.spark.launcher.CommandBuilderUtils.findJarsDir;

/**
 * Patches:
 * 1. Added SPYT_CLASSPATH env variable value to classpath of jvm Spark components.
 */
@Subclass
@OriginClass("org.apache.spark.launcher.AbstractCommandBuilder")
abstract class AbstractCommandBuilderForSpyt extends AbstractCommandBuilder {

    @Override
    List<String> buildClassPath(String appClassPath) throws IOException {
        List<String> classPath = super.buildClassPath(appClassPath);

        // Spyt classpath should have precedence over spark class path, but not over conf directory,
        // so here we're searching for sparkJarsDir and inserting spyt classpath right before it.
        String sparkJarsDir = findJarsDir(getSparkHome(), getScalaVersion(), false);
        int sparkClasspathPos = 0;
        while (sparkClasspathPos < classPath.size() &&
                !classPath.get(sparkClasspathPos).contains(sparkJarsDir)) {
            sparkClasspathPos++;
        }
        classPath.add(sparkClasspathPos, getenv("SPYT_CLASSPATH"));

        return classPath;
    }
}
