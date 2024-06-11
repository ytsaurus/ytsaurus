#include "helpers.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NQueryTrackerClient {

using namespace NQueryTrackerClient::NRecords;
using namespace NYTree;
using namespace NYson;

namespace {

TString FormatAcoList(std::optional<TYsonString> accessControlObjects) {
    if (!accessControlObjects) {
        return "[]";
    }

    auto accessControlObjectsList = ConvertTo<std::vector<TString>>(accessControlObjects);
    for (size_t i = 0; i < accessControlObjectsList.size(); i++) {
        accessControlObjectsList[i] = Format("aco:%v", accessControlObjectsList[i]);
    }

    return ConvertToYsonString(accessControlObjectsList, EYsonFormat::Text).ToString();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): this is terrible, refactor this.

TString GetFilterFactors(const TActiveQueryPartial& record)
{
    return Format("%v %v acos:%v",
        record.Query,
        (record.Annotations && *record.Annotations) ? ConvertToYsonString(*record.Annotations, EYsonFormat::Text).ToString() : "",
        FormatAcoList(record.AccessControlObjects));
}

TString GetFilterFactors(const TFinishedQueryPartial& record)
{
    return Format("%v %v acos:%v",
        record.Query,
        (record.Annotations && *record.Annotations) ? ConvertToYsonString(*record.Annotations, EYsonFormat::Text).ToString() : "",
        FormatAcoList(record.AccessControlObjects));
}

TString GetFilterFactors(const TFinishedQuery& record)
{
    return Format("%v %v acos:%v",
        record.Query,
        record.Annotations ? ConvertToYsonString(record.Annotations, EYsonFormat::Text).ToString() : "",
        FormatAcoList(record.AccessControlObjects));
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

} // namespace NYT::NQueryTrackerClient
