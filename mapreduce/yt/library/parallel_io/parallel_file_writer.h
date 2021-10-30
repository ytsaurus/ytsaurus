#pragma once

#include <mapreduce/yt/interface/client.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// @brief Options for @ref NYT::WriteFileParallel.
struct TParallelFileWriterOptions
    : TIOOptions<TParallelFileWriterOptions>
{
    /// @brief Number of threads.
    FLUENT_FIELD_OPTION(size_t, ThreadCount);

    /// @brief Options for each single-threaded writer.
    FLUENT_FIELD_OPTION(TWriterOptions, WriterOptions);
};

/// @brief Write file in parallel.
/// @param client       Client which used for write file on server.
/// @param fileName     Source path to file in local storage.
/// @param path         Dist path to file on server.
void WriteFileParallel(
    const IClientBasePtr& client,
    const TString& fileName,
    const TRichYPath& path,
    const TParallelFileWriterOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
