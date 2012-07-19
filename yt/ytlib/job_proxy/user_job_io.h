#pragma once

#include "public.h"

#include <ytlib/election/master_discovery.h>
#include <ytlib/table_client/public.h>
#include <ytlib/scheduler/job.pb.h>

class TOutputStream;

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobIO
{
public:
    TUserJobIO(
        TJobIOConfigPtr config,
        NElection::TMasterDiscovery::TConfigPtr mastersConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    virtual ~TUserJobIO()
    { }

    virtual int GetInputCount() const;
    virtual int GetOutputCount() const;

    virtual void UpdateProgress();
    virtual double GetProgress() const;

    virtual TAutoPtr<NTableClient::TTableProducer> CreateTableInput(
        int index, 
        NYTree::IYsonConsumer* consumer) const = 0;

    virtual NTableClient::ISyncWriterPtr CreateTableOutput(int index) const;

    virtual TAutoPtr<TErrorOutput> CreateErrorOutput() const;

protected:
    TJobIOConfigPtr Config;

    NScheduler::NProto::TJobSpec JobSpec;
    NRpc::IChannelPtr MasterChannel;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
