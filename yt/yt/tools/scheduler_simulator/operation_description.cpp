#include "operation_description.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSchedulerSimulator {

using namespace NYTree;
using namespace NYson;
using namespace NPhoenix;
using namespace NScheduler;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

// This class is intended to enable serialization for all subtypes of INode,
// but unfortunately there is no clear way to do this now.
// In future it should be moved to "ytree/serialize.h".
class TYTreeSerializer
{
public:
    static void Save(TStreamSaveContext& context, const IMapNodePtr& node)
    {
        NYT::Save(context, ConvertToYsonString(node).ToString());
    }

    static void Load(TStreamLoadContext& context, IMapNodePtr& node)
    {
        TString str;
        NYT::Load(context, str);
        node = ConvertToNode(TYsonString(str))->AsMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

void TJobDescription::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Duration);
    Persist(context, ResourceLimits);
    Persist(context, Id);
    Persist(context, Type);
    Persist(context, State);
}

void Deserialize(TJobDescription& value, NYTree::INodePtr node)
{
    auto listNode = node->AsList();
    auto duration = listNode->GetChildOrThrow(0)->AsDouble()->GetValue();
    value.Duration = TDuration::MilliSeconds(i64(duration * 1000));
    value.ResourceLimits = {};
    value.ResourceLimits.SetMemory(listNode->GetChildOrThrow(1)->AsInt64()->GetValue());
    value.ResourceLimits.SetCpu(TCpuResource(listNode->GetChildOrThrow(2)->AsDouble()->GetValue()));
    value.ResourceLimits.SetUserSlots(listNode->GetChildOrThrow(3)->AsInt64()->GetValue());
    value.ResourceLimits.SetNetwork(listNode->GetChildOrThrow(4)->AsInt64()->GetValue());
    value.ResourceLimits.SetGpu(0);
    value.Id = TJobId(ConvertTo<TGuid>(listNode->GetChildOrThrow(5)));
    auto jobType = ConvertTo<TString>(listNode->GetChildOrThrow(6));
    if (jobType == "partition_sort") {
        jobType = "intermediate_sort";
    }
    value.Type = ConvertTo<NJobTrackerClient::EJobType>(jobType);
    value.State = ConvertTo<TString>(listNode->GetChildOrThrow(7));
}

////////////////////////////////////////////////////////////////////////////////

void TOperationDescription::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Id);
    Persist(context, JobDescriptions);
    Persist(context, StartTime);
    Persist(context, Duration);
    Persist(context, AuthenticatedUser);
    Persist(context, Type);
    Persist(context, State);
    Persist(context, InTimeframe);
    Persist(context, Spec);
}

void Deserialize(TOperationDescription& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    value.Id = ConvertTo<TOperationId>(mapNode->GetChildOrThrow("operation_id"));
    value.JobDescriptions = ConvertTo<std::vector<TJobDescription>>(mapNode->GetChildOrThrow("job_descriptions"));
    value.StartTime = ConvertTo<TInstant>(mapNode->GetChildOrThrow("start_time"));
    value.Duration = ConvertTo<TInstant>(mapNode->GetChildOrThrow("finish_time")) - value.StartTime;
    value.AuthenticatedUser = ConvertTo<TString>(mapNode->GetChildOrThrow("authenticated_user"));
    value.Type = ConvertTo<NScheduler::EOperationType>(mapNode->GetChildOrThrow("operation_type"));
    value.State = ConvertTo<TString>(mapNode->GetChildOrThrow("state"));
    value.InTimeframe = ConvertTo<bool>(mapNode->GetChildOrThrow("in_timeframe"));
    value.Spec = ConvertToYsonString(mapNode->GetChildOrThrow("spec"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
