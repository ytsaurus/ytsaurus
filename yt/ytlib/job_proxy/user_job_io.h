#pragma once

#include "public.h"

#include <ytlib/rpc/public.h>

#include <ytlib/ytree/public.h>

#include <ytlib/meta_state/public.h>

#include <ytlib/table_client/public.h>

#include <ytlib/chunk_server/public.h>

#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobIO
    : private TNonCopyable
{
public:
    TUserJobIO(
        TJobIOConfigPtr ioConfig,
        NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    virtual ~TUserJobIO();

    virtual int GetInputCount() const;
    virtual int GetOutputCount() const;

    virtual void UpdateProgress();
    virtual double GetProgress() const;

    virtual TAutoPtr<NTableClient::TTableProducer> CreateTableInput(
        int index, 
        NYTree::IYsonConsumer* consumer) const;

    virtual NTableClient::ISyncWriterPtr CreateTableOutput(
        int index) const;

    virtual TAutoPtr<TErrorOutput> CreateErrorOutput() const;

    void SetStderrChunkId(const NChunkServer::TChunkId& chunkId);

    virtual void PopulateResult(NScheduler::NProto::TJobResult* result) = 0;

protected:
    TJobIOConfigPtr IOConfig;
    NRpc::IChannelPtr MasterChannel;
    NScheduler::NProto::TJobSpec JobSpec;

    NChunkServer::TChunkId StderrChunkId;

    void PopulateUserJobResult(NScheduler::NProto::TUserJobResult* result);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
