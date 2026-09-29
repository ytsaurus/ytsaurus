#include "operation_description.h"

#include <yt/yt/core/phoenix/type_def.h>

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
        std::string str;
        NYT::Load(context, str);
        node = ConvertToNode(TYsonString(str))->AsMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

void TJobDescription::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Duration);
    PHOENIX_REGISTER_FIELD(2, ResourceLimits);
    PHOENIX_REGISTER_FIELD(3, Id);
    PHOENIX_REGISTER_FIELD(4, Type);
    PHOENIX_REGISTER_FIELD(5, State);
}

PHOENIX_DEFINE_TYPE(TJobDescription);

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
    auto jobType = ConvertTo<std::string>(listNode->GetChildOrThrow(6));
    if (jobType == "partition_sort") {
        jobType = "intermediate_sort";
    }
    value.Type = ConvertTo<NJobTrackerClient::EJobType>(jobType);
    value.State = ConvertTo<std::string>(listNode->GetChildOrThrow(7));
}

////////////////////////////////////////////////////////////////////////////////

void TOperationDescription::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Id);
    PHOENIX_REGISTER_FIELD(2, JobDescriptions);
    PHOENIX_REGISTER_FIELD(3, StartTime);
    PHOENIX_REGISTER_FIELD(4, Duration);
    PHOENIX_REGISTER_FIELD(5, AuthenticatedUser);
    PHOENIX_REGISTER_FIELD(6, Type);
    PHOENIX_REGISTER_FIELD(7, State);
    PHOENIX_REGISTER_FIELD(8, InTimeframe);
    PHOENIX_REGISTER_FIELD(9, Spec);
}

PHOENIX_DEFINE_TYPE(TOperationDescription);

void Deserialize(TOperationDescription& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    value.Id = ConvertTo<TOperationId>(mapNode->GetChildOrThrow("operation_id"));
    value.JobDescriptions = ConvertTo<std::vector<TJobDescription>>(mapNode->GetChildOrThrow("job_descriptions"));
    value.StartTime = ConvertTo<TInstant>(mapNode->GetChildOrThrow("start_time"));
    value.Duration = ConvertTo<TInstant>(mapNode->GetChildOrThrow("finish_time")) - value.StartTime;
    value.AuthenticatedUser = ConvertTo<std::string>(mapNode->GetChildOrThrow("authenticated_user"));
    value.Type = ConvertTo<NScheduler::EOperationType>(mapNode->GetChildOrThrow("operation_type"));
    value.State = ConvertTo<std::string>(mapNode->GetChildOrThrow("state"));
    value.InTimeframe = ConvertTo<bool>(mapNode->GetChildOrThrow("in_timeframe"));
    value.Spec = ConvertToYsonString(mapNode->GetChildOrThrow("spec"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
