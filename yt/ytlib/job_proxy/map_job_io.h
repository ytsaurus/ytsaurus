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

class TMapJobIo
    : public IUserJobIo
{
public:
    TMapJobIo(
        TJobIoConfigPtr config,
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
    TJobIoConfigPtr Config;

    NScheduler::NProto::TMapJobSpec IoSpec;
    NRpc::IChannel::TPtr MasterChannel;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
