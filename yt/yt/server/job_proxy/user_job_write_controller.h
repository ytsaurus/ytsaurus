#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobWriteController
{
public:
    explicit TUserJobWriteController(IJobHostPtr host);
    ~TUserJobWriteController();

    void Init(TCpuInstant ioStartTime);

    std::vector<IProfilingMultiChunkWriterPtr> GetWriters() const;
    int GetOutputStreamCount() const;
    IOutputStream* GetStderrTableWriter() const;

    std::vector<NTableClient::IValueConsumer*> CreateValueConsumers(
        NTableClient::TTypeConversionConfigPtr typeConversionConfig);
    const std::vector<std::unique_ptr<NTableClient::IFlushableValueConsumer>>& GetAllValueConsumers() const;

    void PopulateResult(NControllerAgent::NProto::TJobResultExt* jobResultExt);
    void PopulateStderrResult(NControllerAgent::NProto::TJobResultExt* jobResultExt);

    std::vector<NChunkClient::IChunkWriter::TWriteBlocksOptions> GetOutputWriteBlocksOptions() const;

protected:
    const IJobHostPtr Host_;
    const NLogging::TLogger Logger;

    std::atomic<bool> Initialized_ = false;

    std::vector<IProfilingMultiChunkWriterPtr> Writers_;
    std::vector<std::unique_ptr<NTableClient::IFlushableValueConsumer>> ValueConsumers_;
    std::unique_ptr<NTableClient::TBlobTableWriter> StderrTableWriter_;

    std::vector<NChunkClient::IChunkWriter::TWriteBlocksOptions> OutputWriteBlocksOptions_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
