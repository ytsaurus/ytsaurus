package tech.ytsaurus.core;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

public final class JavaOptions {

    private static final List<String> CURRENT = ManagementFactory.getRuntimeMXBean().getInputArguments();

    private final List<String> options;

    private JavaOptions(List<String> options) {
        this.options = options;
    }

    public static JavaOptions empty() {
        return new JavaOptions(new ArrayList<>());
    }

    /**
     * Gets actual JVM options.
     */
    public static JavaOptions current() {
        return new JavaOptions(CURRENT);
    }

    public List<String> getOptions() {
        return options;
    }

    public JavaOptions withOption(String value) {
        // --add-* used with '=' and may be used more than once, so skip finding '='
        int eqSignIndex = value.startsWith("--add-") ? -1 : value.indexOf('=');
        int index = eqSignIndex < 0
                ? options.indexOf(value)
                : findOptionIndexStartsWith(value.substring(0, eqSignIndex));
        if (index < 0) {
            List<String> resultOptions = new ArrayList<>(options);
            resultOptions.add(value);
            return new JavaOptions(resultOptions);
        }

        List<String> mutable = new ArrayList<>(options);
        mutable.set(index, value);
        return new JavaOptions(mutable);
    }

    public JavaOptions withoutOption(String value) {
        // --add-* used with '=' and may be used more than once, so skip finding '='
        int eqSignIndex = value.startsWith("--add-") ? -1 : value.indexOf('=');
        if (eqSignIndex < 0) {
            int index = options.indexOf(value);
            if (index < 0) {
                return this;
            }

            List<String> mutable = new ArrayList<>(options);
            mutable.remove(index);
            return new JavaOptions(mutable);
        } else {
            return withoutOptionStartsWith(value.substring(0, eqSignIndex));
        }
    }

    public JavaOptions withoutOptionStartsWith(String value) {
        int index = findOptionIndexStartsWith(value);
        if (index < 0) {
            return this;
        }

        List<String> mutable = new ArrayList<>(options);
        mutable.remove(index);
        return new JavaOptions(mutable);
    }

    /**
     * Sets Xms = Xmx = size.
     */
    public JavaOptions withMemory(DataSize size) {
        return withMemory(size, size);
    }

    /**
     * Sets Xms and Xmx.
     */
    public JavaOptions withMemory(DataSize minSize, DataSize maxSize) {
        return withoutOptionStartsWith("-Xms")
                .withoutOptionStartsWith("-Xmx")
                .withOption("-Xms" + formatMemory(minSize))
                .withOption("-Xmx" + formatMemory(maxSize));
    }

    public JavaOptions withXmx(DataSize xmx) {
        return withoutOptionStartsWith("-Xmx").withOption("-Xmx" + formatMemory(xmx));
    }

    private int findOptionIndexStartsWith(String value) {
        for (int i = 0; i < options.size(); i++) {
            if (options.get(i).startsWith(value)) {
                return i;
            }
        }
        return -1;
    }

    private static String formatMemory(DataSize requiredMemory) {
        return requiredMemory.toMegaBytes() + "m";
    }
}
