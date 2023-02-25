#pragma once

///
/// @file yt/cpp/mapreduce/library/parallel_io/parallel_reader.h
///
/// Functions to read tables in parallel.

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// @brief Options for parallel reading.
struct TParallelTableReaderOptions
    : public TIOOptions<TParallelTableReaderOptions>
{
    /// @brief Whether table rows are returned in order.
    ///
    /// `Ordered == true`: produce rows in the same order as the usual reader.
    /// `Ordered == false`: allow to produce rows in any order.
    ///
    /// @note Unordered mode can be much faster due to ability to read from different chunks and thus from different nodes.
    FLUENT_FIELD_DEFAULT(bool, Ordered, true);

    /// @brief Memory limit allowed to be used by reader.
    ///
    /// Maximum number of rows allowed to be stored in memory
    /// is calculated as `Min(BufferedRowCountLimit, MemoryLimit / AverageRowWeight)`.
    FLUENT_FIELD_DEFAULT(size_t, MemoryLimit, 300'000'000);

    /// @brief Number of created reader threads.
    ///
    /// @brief Actual number of running threads depends on the underlying readers,
    /// e.g. each YSON TNode reader spawns additional parser thread,
    /// so total number of threads will be doubled.
    FLUENT_FIELD_DEFAULT(size_t, ThreadCount, 10);

    /// @brief Allows to fine tune format that is used for reading tables.
    FLUENT_FIELD_OPTION(TFormatHints, FormatHints);

    /// @brief Size of batch to operate with (in bytes).
    FLUENT_FIELD_DEFAULT(size_t, BatchSizeBytes, 4'000'000);

    /// @brief Number of RichYPath ranges each reader will use.
    ///
    /// @note Makes sense only for ordered reader.
    FLUENT_FIELD_DEFAULT(int, RangeCount, 300);

    /// @brief Maximum number of rows allowed to be stored in memory.
    ///
    /// @deprecated
    ///
    /// Together with ThreadCount it determines the read range size
    /// and thus the number of requests that will be issued.
    ///
    /// @note Actual memory consumption depends on the underlying readers,
    /// e.g. each YSON TNode reader allocates additional buffer for parsed rows.
    ///
    /// This field is deprecated, use `MemoryLimit` instead.
    FLUENT_FIELD_DEFAULT(size_t, BufferedRowCountLimit, 1'000'000'000);
};

template <typename T>
struct TParallelRow
{ };

/// @brief Create parallel reader of several tables.
template <typename TRow>
auto CreateParallelTableReader(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    const TParallelTableReaderOptions& options = TParallelTableReaderOptions{});

/// @brief Create parallel reader of a table.
template <typename TRow>
auto CreateParallelTableReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelTableReaderOptions& options = TParallelTableReaderOptions{});

/// @brief Callback to be called (from several threads) for each row batch.
template <typename TRow>
using TParallelReaderRowProcessor = std::function<void(TTableReaderPtr<TRow> reader, int tableIndex)>;

/// @brief Read several tables in parallel and apply `processor` to the rows.
///
/// @param processor Function taking a reader and table index of a batch of rows.
///
/// @note `options.Ordered` must be false. For ordered processing use @ref NYT::CreateParallelTableReader.
/// @note `processor` is called from several threads and thus must be thread-safe.
///
/// Any exception thrown inside `processor` will be propagated to the caller.
template <typename TRow>
void ReadTablesInParallel(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    TParallelReaderRowProcessor<TRow> processor,
    const TParallelTableReaderOptions& options = TParallelTableReaderOptions{});

/// @brief Read a table in parallel and apply `processor` to the rows.
///
/// @see @ref NYT::ReadTablesInParallel.
template <typename TRow>
void ReadTableInParallel(
    const IClientBasePtr& client,
    const TRichYPath& path,
    TParallelReaderRowProcessor<TRow> processor,
    const TParallelTableReaderOptions& options = TParallelTableReaderOptions{});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_READER_INL_H_
#include "parallel_reader-inl.h"
#undef PARALLEL_READER_INL_H_
