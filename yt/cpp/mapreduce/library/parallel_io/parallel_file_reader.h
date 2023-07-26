#pragma once

#include "resource_limiter.h"

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/generic/size_literals.h>
#include <util/memory/blob.h>
#include <util/thread/pool.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// @brief Options for @ref NYT::ReadFileParallel.
struct TParallelFileReaderOptions
    : TIOOptions<TParallelFileReaderOptions>
{
    /// @brief Number of threads.
    FLUENT_FIELD_DEFAULT(size_t, ThreadCount, 5);

    /// @brief Reading unit for each thread
    FLUENT_FIELD_DEFAULT(size_t, BatchSize, 128_MB);

    /// @brief Limits RAM usage of working treads
    /// @note RamLimiter doesn't manage returned data
    FLUENT_FIELD(IResourceLimiterPtr, RamLimiter);

    /// @brief Options for each single-threaded reader.
    FLUENT_FIELD_OPTION(TFileReaderOptions, ReaderOptions);
};

/// @brief Allow to read a file in parallel.
///
/// @note All Read methods are non-threadsafe.
/// @note ReadAll and ReadNextBatch handle memory allocation
/// that doesn't managed by RamLimiter => at the moment, memory usage can be greater than limits
class IParallelFileReader
    : public NYT::IFileReader
{
public:
    /// @brief Read and return next batch. If EOF was seen return std::nullopt
    /// @note Blob size can be less @ref TParallelFileReaderOptions().BatchSize_:
    /// 1. File size is not divisible by @ref TParallelFileReaderOptions().BatchSize_
    /// 2. Sometimes after using @ref Read(void*, size_t) returns piece of batch
    /// @note Return data without copy unlike others read methods
    virtual std::optional<::TBlob> ReadNextBatch() = 0;

protected:
    virtual size_t DoReadTo(TString&, char) final
    {
        ythrow yexception() << "Not implemented! IParallelFileReader explicitly doesn't support read to methods";
    }
};


/// @brief Read file to local file
void SaveFileParallel(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TString& localPath,
    const TParallelFileReaderOptions& options = {});


/// @brief Create parallel reader of a file.
::TIntrusivePtr<IParallelFileReader> CreateParallelFileReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const std::shared_ptr<IThreadPool>& threadPool,
    const TParallelFileReaderOptions& options = {});

/// @brief Create parallel reader of a file.
::TIntrusivePtr<IParallelFileReader> CreateParallelFileReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelFileReaderOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
