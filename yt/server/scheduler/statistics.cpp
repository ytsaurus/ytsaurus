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

void TTotalJobStatistics::Persist(TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Time);
}

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

