#pragma once

#include <mapreduce/yt/interface/client.h>

#include <util/system/thread.h>
#include <util/system/mutex.h>

namespace NYT {

/// @brief options for @ref NYT::WriteFileParallel
struct TParallelFileWriterOptions
    : TIOOptions<TParallelFileWriterOptions>
{
    /// @brief number of threads
    FLUENT_FIELD_OPTION(size_t, NumThreads);

    /// @ref NYT::TWriterOptions
    FLUENT_FIELD_OPTION(TWriterOptions, WriterOptions);
};

/// @brief Write file parallelly.
/// @param client       client which used for write file on server
/// @param fileName     source path to file in local storage
/// @param path         dist path to file on server
void WriteFileParallel(const IClientBasePtr& client, const TString& fileName, const TRichYPath& path,
    const TParallelFileWriterOptions& options = TParallelFileWriterOptions());

} // namespace NYT
