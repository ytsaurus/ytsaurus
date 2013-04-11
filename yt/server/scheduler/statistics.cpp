#include "stdafx.h"
#include "statistics.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////

TTotalJobStatistics::TTotalJobStatistics()
    : Time(TDuration::Zero())
{ }

void Serialize(const TTotalJobStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("time").Value(statistics.Time)
        .EndMap();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

