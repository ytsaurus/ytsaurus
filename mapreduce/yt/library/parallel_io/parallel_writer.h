#pragma once

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

    struct TParallelTableWriterOptions
        : public TTableWriterOptions
    {
        using TSelf = TParallelTableWriterOptions;
        // Number of created writer threads.
        FLUENT_FIELD_DEFAULT(size_t, ThreadCount, 5);
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

