#pragma once

#include "private.h"
#include "public.h"

#include "user_job_io.h"

#include <ytlib/scheduler/job.pb.h>

// ToDo: replace with public.
#include <ytlib/table_client/yson_table_input.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

class TMapJobIO
    : public IUserJobIO
{
public:
    TMapJobIO(
        TJobIOConfigPtr config,
        NRpc::IChannel* masterChannel,
        const NScheduler::NProto::TMapJobSpec& ioSpec);

    int GetInputCount() const;
    int GetOutputCount() const;

    TAutoPtr<NTableClient::TYsonTableInput> CreateTableInput(
        int index, 
        TOutputStream* output) const;
    TAutoPtr<TOutputStream> CreateTableOutput(int index) const;
    TAutoPtr<TOutputStream> CreateErrorOutput() const;

private:
    TJobIOConfigPtr Config;

    NScheduler::NProto::TMapJobSpec IoSpec;
    NRpc::IChannelPtr MasterChannel;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
