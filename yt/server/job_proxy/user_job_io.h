#pragma once

#include "public.h"

#include <ytlib/rpc/public.h>

#include <ytlib/table_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/scheduler/job.pb.h>

#include <ytlib/logging/log.h>

#include <server/chunk_server/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobIO
    : private TNonCopyable
{
public:
    TUserJobIO(
        NScheduler::TJobIOConfigPtr ioConfig,
        IJobHost* host);

    virtual ~TUserJobIO();

    virtual int GetInputCount() const;
    virtual int GetOutputCount() const;

    virtual double GetProgress() const;

    virtual TAutoPtr<NTableClient::TTableProducer> CreateTableInput(
        int index,
        NYson::IYsonConsumer* consumer);

    virtual NTableClient::ISyncWriterPtr CreateTableOutput(
        int index);

    virtual TAutoPtr<TErrorOutput> CreateErrorOutput(
        const NTransactionClient::TTransactionId& transactionId) const;

    void SetStderrChunkId(const NChunkClient::TChunkId& chunkId);
    virtual std::vector<NChunkClient::TChunkId> GetFailedChunks() const;

    virtual void PopulateResult(NScheduler::NProto::TJobResult* result) = 0;

protected:
    NScheduler::TJobIOConfigPtr IOConfig;
    IJobHost* Host;

    NChunkClient::TChunkId StderrChunkId;

    std::vector<NTableClient::ISyncReaderPtr> Inputs;
    std::vector<NTableClient::TTableChunkWriterProviderPtr> Outputs;

    NLog::TLogger& Logger;

    template <template <typename> class TMultiChunkReader>
    TAutoPtr<NTableClient::TTableProducer> DoCreateTableInput(
        int index,
        NYson::IYsonConsumer* consumer);

    void PopulateUserJobResult(NScheduler::NProto::TUserJobResult* result);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

#define USER_JOB_IO_INL_H_
#include "user_job_io-inl.h"
#undef USER_JOB_IO_INL_H_

