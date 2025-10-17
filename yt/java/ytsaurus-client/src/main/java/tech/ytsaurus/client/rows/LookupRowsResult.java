package tech.ytsaurus.client.rows;

import java.util.List;
import java.util.Objects;

/**
 * Result of a lookup rows operation.
 * <p>
 * Contains the rowset with the lookup results and optionally unavailable key indexes
 * if partial results are enabled.
 *
 * @param <T> the type of the rowset
 */
public class LookupRowsResult<T> {
    private final T rowset;
    private final List<Integer> unavailableKeyIndexes;

    /**
     * Create a lookup result with a rowset and unavailable key indexes.
     *
     * @param rowset the rowset containing the lookup results
     * @param unavailableKeyIndexes list of indexes of keys that were not available
     */
    public LookupRowsResult(T rowset, List<Integer> unavailableKeyIndexes) {
        this.rowset = Objects.requireNonNull(rowset);
        this.unavailableKeyIndexes = Objects.requireNonNull(unavailableKeyIndexes);
    }

    /**
     * Get the rowset containing the lookup results.
     *
     * @return the rowset
     */
    public T getRowset() {
        return rowset;
    }

    /**
     * Get the list of unavailable key indexes.
     * <p>
     * If enablePartialResult is set in the request, this list contains
     * indexes of keys that were not available due to timeout or other failures.
     * If keepMissingRows is false, then the corresponding rows are omitted
     * from the rowset. Otherwise these rows are present but are null.
     * <p>
     * The indexes are guaranteed to be unique and increasing.
     *
     * @return list of unavailable key indexes
     */
    public List<Integer> getUnavailableKeyIndexes() {
        return unavailableKeyIndexes;
    }

    /**
     * Check if there are any unavailable keys.
     *
     * @return true if there are unavailable keys, false otherwise
     */
    public boolean hasUnavailableKeys() {
        return !unavailableKeyIndexes.isEmpty();
    }

    @Override
    public String toString() {
        return "LookupRowsResult{" +
                "rowset=" + rowset +
                ", unavailableKeyIndexes=" + unavailableKeyIndexes +
                '}';
    }
}