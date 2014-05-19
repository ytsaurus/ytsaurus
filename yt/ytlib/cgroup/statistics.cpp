#include "stdafx.h"
#include "statistics.h"

#include <core/ytree/fluent.h>

namespace NYT {
namespace NCGroup {
namespace NProto {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TCpuAccountingStats& statistics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("user_time").Value(statistics.user_time())
            .Item("system_time").Value(statistics.system_time())
        .EndMap();
}

void Serialize(const TBlockIOStats& statistics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total_sectors").Value(statistics.total_sectors())
            .Item("bytes_read").Value(statistics.bytes_read())
            .Item("bytes_written").Value(statistics.bytes_written())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto
} // namespace NCGroup
} // namespace NYT
