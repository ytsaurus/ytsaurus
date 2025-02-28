#pragma once

#include "resource_limiter.h"

#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/thread/pool.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// @brief Options for @ref NYT::WriteFileParallel.
struct TParallelFileWriterOptions
    : TIOOptions<TParallelFileWriterOptions>
{
    /// @brief Number of threads.
    FLUENT_FIELD_DEFAULT(size_t, ThreadCount, 5);

    FLUENT_FIELD_DEFAULT(size_t, MaxBlobSize, (128ull * 1ull << 20ull)); // 128 MiB

    FLUENT_FIELD(IResourceLimiterPtr, RamLimiter);

    FLUENT_FIELD_DEFAULT(bool, AcquireRamForBuffers, false);

    /// @brief Directory for temporary files. By default: directory of the output file.
    FLUENT_FIELD_OPTION(TYPath, TmpDirectory);

    /// @brief Attributes to set for written file.
    /// Useful for changing node attributes, for example, "primary_medium"="ssd_blobs".
    /// Applies to both temporary nodes (speeding up write) and final node.
    FLUENT_FIELD_DEFAULT(NYT::TNode, FileAttributes, NYT::TNode::CreateMap());

    ///
    /// @brief Wheter to call Finish automatically in writer destructor.
    ///
    /// If set to true (default) Finish() is called automatically in the destructor of writer.
    /// It is convenient for simple usecases but might be error-prone if writing exception safe code
    /// (In case of exceptions it's common to abort writer and not commit partial data).
    ///
    /// If set to false Finish() has to be called explicitly.
    FLUENT_FIELD_DEFAULT(bool, AutoFinish, true);

    /// @brief Options for each single-threaded writer.
    FLUENT_FIELD_OPTION(TWriterOptions, WriterOptions);
};

/// @brief Allow to write a whole file in parallel.
///
/// In the result file all parts will be written in order of call Write/WriteFile methods.
class IParallelFileWriter
    : public TThrRefBase
{
public:
    /// @brief Start writing data from the passed blob, don't wait for its finishing.
    virtual void Write(TSharedRef blob) = 0;

    /// @brief Start writing data from file located on path `fileName`,
    ///        don't wait for its finishing.
    /// @param fileName     Source path to file in local storage.
    virtual void WriteFile(const TString& fileName) = 0;

    /// @brief Wait for the finishing of all write threads and concatenate all written parts.
    virtual void Finish() = 0;
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

/// @brief Create parallel writer of a file.
::TIntrusivePtr<IParallelFileWriter> CreateParallelFileWriter(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelFileWriterOptions& options = {});

/// @brief Create parallel writer of a file.
::TIntrusivePtr<IParallelFileWriter> CreateParallelFileWriter(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const std::shared_ptr<IThreadPool>& threadPool,
    const TParallelFileWriterOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
