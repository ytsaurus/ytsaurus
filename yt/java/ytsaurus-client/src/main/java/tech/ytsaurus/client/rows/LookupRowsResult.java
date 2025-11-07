package tech.ytsaurus.client.rows;

import java.util.List;
import java.util.Objects;

abstract class LookupRowsResult {
    private final List<Integer> unavailableKeyIndexes;

    LookupRowsResult(List<Integer> unavailableKeyIndexes) {
        this.unavailableKeyIndexes = Objects.requireNonNull(unavailableKeyIndexes);
    }

    public List<Integer> getUnavailableKeyIndexes() {
        return unavailableKeyIndexes;
    }

    public boolean hasUnavailableKeys() {
        return !unavailableKeyIndexes.isEmpty();
    }
}
