#pragma once

#include "private.h"
#include "public.h"

#include <ytlib/scheduler/jobs.pb.h>

// ToDo: replace with public.
#include <ytlib/election/leader_lookup.h>
#include <ytlib/table_client/yson_table_input.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

// ToDo: Convert to interface with factory depending on job type in future.
class TJobSpec
{
public:
    TJobSpec(
        TJobIoConfig* config,
        NElection::TLeaderLookup::TConfig* mastersConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    int GetInputCount() const;
    int GetOutputCount() const;

    TAutoPtr<NTableClient::TYsonTableInput> GetInputTable(int index, TOutputStream* output);
    TAutoPtr<TOutputStream> GetOutputTable(int index);
    TAutoPtr<TOutputStream> GetErrorOutput();

    const Stroka& GetShellCommand() const;

private:
    TJobIoConfigPtr Config;

    NScheduler::NProto::TMapJobSpec MapJobSpec;
    NScheduler::NProto::TUserJobSpec UserJobSpec;

    NRpc::IChannel::TPtr MasterChannel;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
