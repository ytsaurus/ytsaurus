#pragma once

#include "private.h"
#include "public.h"

#include "user_job_io.h"

#include <ytlib/scheduler/job.pb.h>
#include <ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

class TMapJobIO
    : public IUserJobIO
{
public:
    TMapJobIO(
        TJobIOConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        const NScheduler::NProto::TJobSpec& jobSpec);

    int GetInputCount() const;
    int GetOutputCount() const;

    void UpdateProgress();
    double GetProgress() const;

    TAutoPtr<NTableClient::TTableProducer> CreateTableInput(
        int index, 
        NYTree::IYsonConsumer* consumer) const;
    NTableClient::ISyncWriterPtr CreateTableOutput(int index) const;
    TAutoPtr<TErrorOutput> CreateErrorOutput() const;

private:
    TJobIOConfigPtr Config;

    NScheduler::NProto::TJobSpec JobSpec;
    NRpc::IChannelPtr MasterChannel;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
