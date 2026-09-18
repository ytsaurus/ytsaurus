package tech.ytsaurus.flow.state;

/**
 * Wire format of external state payloads, mirroring the {@code TState.format} protocol field
 * and the worker-side {@code EStateFormat} names.
 */
public enum StateFormat {
    /**
     * Payloads are wire-serialized unversioned rows (the historical default).
     */
    SIMPLE_ROW(0, "simple_row"),
    /**
     * Payloads are serialized protobuf messages.
     */
    PROTO(1, "proto");

    private final int wireValue;
    private final String formatName;

    StateFormat(int wireValue, String formatName) {
        this.wireValue = wireValue;
        this.formatName = formatName;
    }

    /**
     * Returns the {@code TState.format} wire value of this format.
     */
    public int getWireValue() {
        return wireValue;
    }

    /**
     * Returns the format name as advertised in {@code supported_state_formats}.
     */
    public String getFormatName() {
        return formatName;
    }

    /**
     * Resolves a format from its {@code TState.format} wire value.
     *
     * @param wireValue the wire value
     * @return the format
     * @throws IllegalArgumentException if the value is unknown
     */
    public static StateFormat fromWireValue(int wireValue) {
        for (StateFormat format : values()) {
            if (format.wireValue == wireValue) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown state format wire value: " + wireValue);
    }
}
