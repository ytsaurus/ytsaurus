package tech.ytsaurus.flow.state;

import tech.ytsaurus.flow.row.Keyed;
import tech.ytsaurus.flow.row.Payload;

/**
 * {@link StateDescriptor} for an external state (must be pre-configured in the computation spec).
 */
public final class ExternalStateDescriptor extends StateDescriptor<Payload> {
    private final String name;

    ExternalStateDescriptor(String name) {
        validateExternalStateName(name);
        this.name = name;
    }

    /**
     * Validates an external state name.
     *
     * <p>External state names must be absolute paths that mirror the keys used in
     * {@code external_state_managers} of the pipeline spec, so this rejects names
     * that do not start with {@code "/"}, are empty, end with {@code "/"}, equal
     * {@code "/"} or contain two adjacent {@code "/"}.
     *
     * @param name external state name to validate
     */
    static void validateExternalStateName(String name) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("External state name is empty");
        }
        if (name.equals("/")) {
            throw new IllegalArgumentException("External state name is root: " + name);
        }
        if (!name.startsWith("/")) {
            throw new IllegalArgumentException(
                    "External state name does not start with '/': " + name);
        }
        if (name.endsWith("/")) {
            throw new IllegalArgumentException(
                    "External state name ends with '/': " + name);
        }
        int doubleSlash = name.indexOf("//");
        if (doubleSlash != -1) {
            throw new IllegalArgumentException(
                    "External state name contains two adjacent '/' at position "
                            + doubleSlash + ": " + name);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Class<Payload> getStateClass() {
        return Payload.class;
    }

    @Override
    ExternalStateAccessor create(Keyed key, StateBackend backend) {
        return new ExternalStateAccessor(key.getKey(), backend.getExternalStateHolder(name));
    }
}
