package org.apache.spark.launcher;

import tech.ytsaurus.spyt.patch.annotations.OriginClass;
import tech.ytsaurus.spyt.patch.annotations.Subclass;

import java.io.IOException;
import java.util.List;

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
        classPath.add(getenv("SPYT_CLASSPATH"));
        return classPath;
    }
}
