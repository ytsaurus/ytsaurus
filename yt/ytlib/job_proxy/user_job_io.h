#pragma once

#include "public.h"

#include <ytlib/election/leader_lookup.h>
#include <ytlib/table_client/public.h>
#include <ytlib/scheduler/job.pb.h>

class TOutputStream;

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserJobIO
{
    virtual ~IUserJobIO()
    { }

    virtual int GetInputCount() const = 0;
    virtual int GetOutputCount() const = 0;

    virtual void UpdateProgress() = 0;
    virtual double GetProgress() const = 0;

    virtual TAutoPtr<NTableClient::TTableProducer> CreateTableInput(
        int index, 
        NYTree::IYsonConsumer* consumer) const = 0;

    virtual NTableClient::ISyncWriterPtr CreateTableOutput(int index) const = 0;

    virtual TAutoPtr<TErrorOutput> CreateErrorOutput() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<IUserJobIO> CreateUserJobIO(
    TJobIOConfigPtr ioConfig,
    NElection::TLeaderLookup::TConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT