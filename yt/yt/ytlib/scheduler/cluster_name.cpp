#include "cluster_name.h"

#include <library/cpp/yt/string/format.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

bool IsLocal(const TClusterName& clusterName)
{
    return clusterName.Underlying().empty();
}

void FormatValue(TStringBuilderBase* builder, const TClusterName& name, TStringBuf spec)
{
    FormatValue(builder, name.Underlying().empty() ? "<local>" : name.Underlying(), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
