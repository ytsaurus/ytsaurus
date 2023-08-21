package tech.ytsaurus.client.operations;

import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public interface MapperOrReducer<TInput, TOutput> {

    default void start(Yield<TOutput> yield, Statistics statistics) {
        String jobName;
        Class<?> enclosingClass = getClass().getEnclosingClass();
        if (enclosingClass != null) {
            jobName = enclosingClass.getSimpleName();
        } else {
            jobName = getClass().getSimpleName();
        }
        statistics.start(jobName);
    }

    default void finish(Yield<TOutput> yield, Statistics statistics) {
        statistics.finish();
    }

    default YTableEntryType<TInput> inputType() {
        return YTableEntryTypeUtils.resolve(this, 0);
    }

    default YTableEntryType<TOutput> outputType() {
        return YTableEntryTypeUtils.resolve(this, 1);
    }

    /**
     * @return true if you want to get actual row and table indices in OperationContext,
     * false otherwise (tableIndex stays at 0, rowIndex starts at 0 and is incremented on each entry)
     * @see OperationContext
     */
    default boolean trackIndices() {
        return false;
    }
}
