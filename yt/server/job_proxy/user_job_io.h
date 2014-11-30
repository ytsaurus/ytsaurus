#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/table_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/chunk_client/data_statistics.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/scheduler/job.pb.h>

#include <core/logging/log.h>

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

    virtual std::unique_ptr<NTableClient::TTableProducer> CreateTableInput(
        int index,
        NYson::IYsonConsumer* consumer);

    virtual NTableClient::ISyncWriterPtr CreateTableOutput(
        int index);

    virtual std::unique_ptr<TErrorOutput> CreateFailContextOutput(
        const NTransactionClient::TTransactionId& transactionId) const;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const;

    virtual void PopulateResult(NJobTrackerClient::NProto::TJobResult* result) = 0;

    virtual NChunkClient::NProto::TDataStatistics GetInputDataStatistics() const;
    virtual NChunkClient::NProto::TDataStatistics GetOutputDataStatistics() const;

protected:
    NScheduler::TJobIOConfigPtr IOConfig;
    IJobHost* Host;

    const NJobTrackerClient::NProto::TJobSpec& JobSpec;
    const NScheduler::NProto::TSchedulerJobSpecExt& SchedulerJobSpecExt;

    // Protects Inputs and Outputs.
    TSpinLock SpinLock;
    std::vector<NTableClient::ISyncReaderPtr> Inputs;
    std::vector<NTableClient::TTableChunkSequenceWriterPtr> Outputs;

    NLog::TLogger& Logger;

    template <template <typename> class TMultiChunkReader>
    std::unique_ptr<NTableClient::TTableProducer> DoCreateTableInput(
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

