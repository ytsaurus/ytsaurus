#pragma once

#include "private.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

i64 GetFootprintMemorySize();

i64 GetInputIOMemorySize(
    const NScheduler::TJobIOConfigPtr& ioConfig,
    const NTableClient::TChunkStripeStatistics& stat);

i64 GetSortInputIOMemorySize(const NTableClient::TChunkStripeStatistics& stat);

i64 GetIntermediateOutputIOMemorySize(const NScheduler::TJobIOConfigPtr& ioConfig);

i64 GetOutputWindowMemorySize(const NScheduler::TJobIOConfigPtr& ioConfig);

////////////////////////////////////////////////////////////////////////////////

class TOverrunTableWriteBufferMemoryInfo
{
public:
    TOverrunTableWriteBufferMemoryInfo() = default;
    TOverrunTableWriteBufferMemoryInfo(const TOverrunTableWriteBufferMemoryInfo& other) = default;
    TOverrunTableWriteBufferMemoryInfo& operator=(const TOverrunTableWriteBufferMemoryInfo& other) = default;

    TOverrunTableWriteBufferMemoryInfo(
        TJobId jobId,
        i64 reservedMemoryForJobProxyWithFixedBuffer,
        i64 reservedMemoryForJobProxyWithEstimatedBuffer);

    TJobId GetJobId() const;
    i64 GetReservedMemoryForJobProxyWithFixedBuffer() const;
    i64 GetReservedMemoryForJobProxyWithEstimatedBuffer() const;
    double GetRelativeDifference() const;

    std::strong_ordering operator <=> (const TOverrunTableWriteBufferMemoryInfo& other) const;

    void Persist(const NPhoenix::TPersistenceContext& context);

private:
    TJobId JobId_;
    i64 ReservedMemoryForJobProxyWithFixedBuffer_ = 0;
    i64 ReservedMemoryForJobProxyWithEstimatedBuffer_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
