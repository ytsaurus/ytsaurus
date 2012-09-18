#pragma once

#include "public.h"

#include <ytlib/rpc/public.h>

#include <ytlib/ytree/public.h>

#include <ytlib/meta_state/public.h>

#include <ytlib/table_client/public.h>

#include <server/chunk_server/public.h>

#include <ytlib/scheduler/job.pb.h>
#include <ytlib/logging/log.h>

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

    virtual double GetProgress() const;

    virtual TAutoPtr<NTableClient::TTableProducer> CreateTableInput(
        int index, 
        NYTree::IYsonConsumer* consumer);

    virtual NTableClient::ISyncWriterPtr CreateTableOutput(
        int index) const;

    virtual TAutoPtr<TErrorOutput> CreateErrorOutput() const;

    void SetStderrChunkId(const NChunkClient::TChunkId& chunkId);

    virtual void PopulateResult(NScheduler::NProto::TJobResult* result) = 0;

protected:
    TJobIOConfigPtr IOConfig;
    NRpc::IChannelPtr MasterChannel;
    NScheduler::NProto::TJobSpec JobSpec;

    NChunkClient::TChunkId StderrChunkId;

    std::vector<NTableClient::ISyncReaderPtr> Inputs;

    NLog::TLogger& Logger;

    template <template <typename> class TMultiChunkReader>
    TAutoPtr<NTableClient::TTableProducer> DoCreateTableInput(
        int index, 
        NYTree::IYsonConsumer* consumer);

    void PopulateUserJobResult(NScheduler::NProto::TUserJobResult* result);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

#define USER_JOB_IO_INL_H_
#include "user_job_io-inl.h"
#undef USER_JOB_IO_INL_H_

