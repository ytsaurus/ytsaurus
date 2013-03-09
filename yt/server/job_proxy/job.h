#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>

#include <ytlib/rpc/public.h>

#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

//! Represents a context for running jobs inside job proxy.
struct IJobHost
{
    virtual ~IJobHost()
    { }

    virtual TJobProxyConfigPtr GetConfig() = 0;
    virtual const NScheduler::NProto::TJobSpec& GetJobSpec() const = 0;

    virtual const NScheduler::NProto::TNodeResources& GetResourceUsage() const = 0;
    virtual void SetResourceUsage(const NScheduler::NProto::TNodeResources& usage) = 0;

    virtual void ReleaseNetwork() = 0;

    virtual NRpc::IChannelPtr GetMasterChannel() const = 0;

    virtual NChunkClient::IBlockCachePtr GetBlockCache() const = 0;

    virtual NChunkClient::TNodeDirectoryPtr GetNodeDirectory() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

//! Represents a job running inside job proxy.
struct IJob
    : public virtual TRefCounted
{
    virtual ~IJob()
    { }

    virtual NScheduler::NProto::TJobResult Run() = 0;
    
    virtual std::vector<NChunkClient::TChunkId> GetFailedChunks() const = 0;
    
    virtual double GetProgress() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
