#include "operation_description.h"

#include <yt/core/ytree/convert.h>

namespace NYT::NSchedulerSimulator {

using namespace NYTree;
using namespace NYson;
using namespace NPhoenix;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

// This class is intended to enable serialization for all subtypes of INode,
// but unfortunately there is no clear way to do this now.
// In future it should be moved to "ytree/serialize.h".
class TYTreeSerializer
{
public:
    static void Save(TSaveContext& context, const IMapNodePtr& node)
    {
        NYT::Save(context, ConvertToYsonString(node).GetData());
    }

    static void Load(TLoadContext& context, IMapNodePtr& node)
    {
        TString str;
        NYT::Load(context, str);
        node = ConvertToNode(TYsonString(str))->AsMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

void TJobDescription::Persist(const TPersistenceContext& context)
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
    auto duration = listNode->GetChild(0)->AsDouble()->GetValue();
    value.Duration = TDuration::MilliSeconds(i64(duration * 1000));
    value.ResourceLimits = ZeroJobResources();
    value.ResourceLimits.SetMemory(listNode->GetChild(1)->AsInt64()->GetValue());
    value.ResourceLimits.SetCpu(TCpuResource(listNode->GetChild(2)->AsDouble()->GetValue()));
    value.ResourceLimits.SetUserSlots(listNode->GetChild(3)->AsInt64()->GetValue());
    value.ResourceLimits.SetNetwork(listNode->GetChild(4)->AsInt64()->GetValue());
    value.ResourceLimits.SetGpu(0);
    value.Id = ConvertTo<TGuid>(listNode->GetChild(5));
    auto jobType = ConvertTo<TString>(listNode->GetChild(6));
    if (jobType == "partition_sort") {
        jobType = "intermediate_sort";
    }
    value.Type = ConvertTo<NJobTrackerClient::EJobType>(jobType);
    value.State = ConvertTo<TString>(listNode->GetChild(7));
}

////////////////////////////////////////////////////////////////////////////////

void TOperationDescription::Persist(const TPersistenceContext& context)
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
    Persist<TYTreeSerializer>(context, Spec);
}

void Deserialize(TOperationDescription& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    value.Id = ConvertTo<TGuid>(mapNode->GetChild("operation_id"));
    value.JobDescriptions = ConvertTo<std::vector<TJobDescription>>(mapNode->GetChild("job_descriptions"));
    value.StartTime = ConvertTo<TInstant>(mapNode->GetChild("start_time"));
    value.Duration = ConvertTo<TInstant>(mapNode->GetChild("finish_time")) - value.StartTime;
    value.AuthenticatedUser = ConvertTo<TString>(mapNode->GetChild("authenticated_user"));
    value.Type = ConvertTo<NScheduler::EOperationType>(mapNode->GetChild("operation_type"));
    value.State = ConvertTo<TString>(mapNode->GetChild("state"));
    value.InTimeframe = ConvertTo<bool>(mapNode->GetChild("in_timeframe"));
    value.Spec = mapNode->GetChild("spec")->AsMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
