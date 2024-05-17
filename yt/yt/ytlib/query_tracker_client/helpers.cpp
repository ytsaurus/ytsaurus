#include "helpers.h"

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <yt/yt/client/api/query_tracker_client.h>
#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

namespace NYT::NQueryTrackerClient {

using namespace NQueryTrackerClient::NRecords;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): this is terrible, refactor this.

TString GetFilterFactors(const TActiveQueryPartial& record)
{
    return Format("%v %v aco:%v",
        record.Query,
        (record.Annotations && *record.Annotations) ? ConvertToYsonString(*record.Annotations, EYsonFormat::Text).ToString() : "",
        (record.AccessControlObject && *record.AccessControlObject) ? **record.AccessControlObject : "");
}

TString GetFilterFactors(const TFinishedQueryPartial& record)
{
    return Format("%v %v aco:%v",
        record.Query,
        (record.Annotations && *record.Annotations) ? ConvertToYsonString(*record.Annotations, EYsonFormat::Text).ToString() : "",
        (record.AccessControlObject && *record.AccessControlObject) ? **record.AccessControlObject : "");
}

TString GetFilterFactors(const TFinishedQuery& record)
{
    return Format("%v %v aco:%v",
        record.Query,
        record.Annotations ? ConvertToYsonString(record.Annotations, EYsonFormat::Text).ToString() : "",
        record.AccessControlObject ? *record.AccessControlObject : "");
}

////////////////////////////////////////////////////////////////////////////////

bool IsPreFinishedState(EQueryState state)
{
    return state == EQueryState::Aborting || state == EQueryState::Failing || state == EQueryState::Completing;
}

bool IsFinishedState(EQueryState state)
{
    return state == EQueryState::Aborted || state == EQueryState::Failed ||
        state == EQueryState::Completed || state == EQueryState::Draft;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQuery* protoQuery, const NApi::TQuery& query)
{
    protoQuery->Clear();

    ToProto(protoQuery->mutable_id(), query.Id);

    if (query.Engine) {
        protoQuery->set_engine(NApi::NRpcProxy::NProto::ConvertQueryEngineToProto(*query.Engine));
    }
    if (query.Query) {
        protoQuery->set_query(*query.Query);
    }
    if (query.Files) {
        protoQuery->set_files(query.Files->ToString());
    }
    if (query.StartTime) {
        protoQuery->set_start_time(NYT::ToProto<i64>(*query.StartTime));
    }
    if (query.FinishTime) {
        protoQuery->set_finish_time(NYT::ToProto<i64>(*query.FinishTime));
    }
    if (query.Settings) {
        protoQuery->set_settings(query.Settings.ToString());
    }
    if (query.User) {
        protoQuery->set_user(*query.User);
    }
    if (query.AccessControlObject) {
        protoQuery->set_access_control_object(*query.AccessControlObject);
    }
    if (query.State) {
        protoQuery->set_state(NApi::NRpcProxy::NProto::ConvertQueryStateToProto(*query.State));
    }
    if (query.ResultCount) {
        protoQuery->set_result_count(*query.ResultCount);
    }
    if (query.Progress) {
        protoQuery->set_progress(query.Progress.ToString());
    }
    if (query.Error) {
        ToProto(protoQuery->mutable_error(), *query.Error);
    }
    if (query.Annotations) {
        protoQuery->set_annotations(query.Annotations.ToString());
    }
    if (query.OtherAttributes) {
        ToProto(protoQuery->mutable_other_attributes(), *query.OtherAttributes);
    }
}

void FromProto(NApi::TQuery* query, const NProto::TQuery& protoQuery)
{
    FromProto(&query->Id, protoQuery.id());

    if (protoQuery.has_engine()) {
        query->Engine = NApi::NRpcProxy::NProto::ConvertQueryEngineFromProto(protoQuery.engine());
    } else {
        query->Engine.reset();
    }
    if (protoQuery.has_query()) {
        query->Query = protoQuery.query();
    } else {
        query->Query.reset();
    }
    if (protoQuery.has_files()) {
        query->Files = TYsonString(protoQuery.files());
    } else {
        query->Files.reset();
    }
    if (protoQuery.has_start_time()) {
        query->StartTime = TInstant::FromValue(protoQuery.start_time());
    } else {
        query->StartTime.reset();
    }
    if (protoQuery.has_finish_time()) {
        query->FinishTime = TInstant::FromValue(protoQuery.finish_time());
    } else {
        query->FinishTime.reset();
    }
    if (protoQuery.has_settings()) {
        query->Settings = TYsonString(protoQuery.settings());
    } else {
        query->Settings = TYsonString{};
    }
    if (protoQuery.has_user()) {
        query->User = protoQuery.user();
    } else {
        query->User.reset();
    }
    if (protoQuery.has_access_control_object()) {
        query->AccessControlObject = protoQuery.access_control_object();
    } else {
        query->AccessControlObject.reset();
    }
    if (protoQuery.has_state()) {
        query->State = NApi::NRpcProxy::NProto::ConvertQueryStateFromProto(protoQuery.state());
    } else {
        query->State.reset();
    }
    if (protoQuery.has_result_count()) {
        query->ResultCount = protoQuery.result_count();
    } else {
        query->ResultCount.reset();
    }
    if (protoQuery.has_progress()) {
        query->Progress = TYsonString(protoQuery.progress());
    } else {
        query->Progress = TYsonString{};
    }
    if (protoQuery.has_error()) {
        query->Error = FromProto<TError>(protoQuery.error());
    } else {
        query->Error.reset();
    }
    if (protoQuery.has_annotations()) {
        query->Annotations = TYsonString(protoQuery.annotations());
    } else {
        query->Annotations = TYsonString{};
    }
    if (protoQuery.has_other_attributes()) {
        query->OtherAttributes = NYTree::FromProto(protoQuery.other_attributes());
    } else if (query->OtherAttributes) {
        query->OtherAttributes->Clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
