#include "helpers.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

namespace NYT::NQueryTrackerClient {

using namespace NQueryTrackerClient::NRecords;
using namespace NYTree;
using namespace NYson;

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

} // namespace NYT::NQueryTrackerClient
