#include "helpers.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

namespace NYT::NQueryTrackerClient {

using namespace NQueryTrackerClient::NRecords;
using namespace NYTree;
using namespace NYson;

//////////////////////////////////////////////////////////////////////////////

TString GetFilterFactors(const TActiveQueryPartial& record)
{
    return Format("%v %v", record.Query, ConvertToYsonString(*record.Settings, EYsonFormat::Text).ToString());
}

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
