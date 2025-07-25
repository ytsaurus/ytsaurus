#include "cluster_name.h"

#include <library/cpp/yt/string/format.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

bool IsLocal(const TClusterName& clusterName)
{
    return !clusterName.Underlying();
}

void FormatValue(TStringBuilderBase* builder, const TClusterName& name, TStringBuf spec)
{
    FormatValue(builder, name.Underlying().value_or("<local>"), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
