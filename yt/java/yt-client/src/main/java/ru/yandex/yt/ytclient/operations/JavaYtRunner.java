package ru.yandex.yt.ytclient.operations;

import java.util.List;

import javax.annotation.Nullable;


import tech.ytsaurus.core.JavaOptions;
public class JavaYtRunner {
    protected JavaYtRunner() {
    }

    public static String normalizeClassName(String clazz) {
        return clazz.replace("$", "dollar_char");
    }

    public static String denormalizeClassName(String name) {
        return name.replace("dollar_char", "$");
    }

    public static String command(
            String javaBinary,
            String classPath,
            @Nullable String libraryPath,
            JavaOptions javaOptions,
            String mainClazz,
            List<String> args) {
        String javaPath = "-cp " + classPath;
        if (libraryPath != null) {
            javaPath += " -Djava.library.path=" + libraryPath;
        }
        return javaBinary + " " + String.join(" ", javaOptions.getOptions()) + " " + javaPath + " "
                + normalizeClassName(mainClazz) + (args.isEmpty() ? "" : " " + String.join(" ", args));
    }
}
