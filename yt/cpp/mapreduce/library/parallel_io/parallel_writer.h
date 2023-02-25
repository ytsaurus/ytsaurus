#pragma once

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

    struct TParallelTableWriterOptions
        : public TIOOptions<TParallelTableWriterOptions>
    {
        // Number of created writer threads.
        FLUENT_FIELD_DEFAULT(size_t, ThreadCount, 5);

        // ParallelWriter has a queue for writing tasks (it can be a row and a vector of rows which should be written).
        // It is a limit of this queue.
        // Set to 0 for unlimited size of queue.
        FLUENT_FIELD_DEFAULT(size_t, TaskCount, 1000);

        // @ref NYT::TTableWriterOptions
        FLUENT_FIELD_DEFAULT(TTableWriterOptions, TableWriterOptions, TTableWriterOptions());
    };

    // Create writer that uses multiple threads to write to a table.
    // It should work faster than ordinary writer but it can mix the order of rows.
    //
    // To gain better performance it is recommended to use AddRow(T&&) instead of AddRow(const T&).

    template <typename T>
    TTableWriterPtr<T> CreateParallelUnorderedTableWriter(
        const IClientBasePtr& client,
        const TRichYPath& path,
        const TParallelTableWriterOptions& options = TParallelTableWriterOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_WRITER_INL_H_
#include "parallel_writer-inl.h"
#undef PARALLEL_WRITER_INL_H_

