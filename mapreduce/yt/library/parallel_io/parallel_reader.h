#pragma once

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TParallelTableReaderOptions
    : public TIOOptions<TParallelTableReaderOptions>
{
    // Ordered == true: produce rows in the same order as the usual reader.
    // Ordered == false: allow to produce rows in any order.
    //
    // NOTE: Unordered mode can be much faster
    // due to ability to read from different chunks and thus from different nodes.
    FLUENT_FIELD_DEFAULT(bool, Ordered, true);

    // Maximum number of rows allowed to be stored in memory.
    // Together with ThreadCount it determines the read range size
    // and thus the number of requests that will be issued.
    //
    // NOTE: Actual memory consumption depends on the underlying readers,
    // e.g. each YSON TNode reader allocates additional buffer for parsed rows.
    //
    // This field is deprecated, use `MemoryLimit` instead
    FLUENT_FIELD_DEFAULT(size_t, BufferedRowCountLimit, 1'000'000);

    // Maximum number of rows allowed to be stored in memory
    // is calculated as Min(BufferedRowCountLimit, MemoryLimit / "Average weight row in table")
    FLUENT_FIELD_DEFAULT(size_t, MemoryLimit, 300'000'000);

    // Number of created reader threads.
    //
    // NOTE: Actual number of running threads depends on the underlying readers,
    // e.g. each YSON TNode reader spawns additional parser thread,
    // so total number of threads will be doubled.
    FLUENT_FIELD_DEFAULT(size_t, ThreadCount, 10);

    // Allows to fine tune format that is used for reading tables.
    FLUENT_FIELD_OPTION(TFormatHints, FormatHints);
};

template <typename T>
TTableReaderPtr<T> CreateParallelTableReader(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    const TParallelTableReaderOptions& options = TParallelTableReaderOptions());

template <typename T>
TTableReaderPtr<T> CreateParallelTableReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelTableReaderOptions& options = TParallelTableReaderOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_READER_INL_H_
#include "parallel_reader-inl.h"
#undef PARALLEL_READER_INL_H_
