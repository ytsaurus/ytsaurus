#include "stdafx.h"
#include "statistics.h"

#include <core/ytree/fluent.h>

namespace NYT {
namespace NCGroup {
namespace NProto {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TCpuAccountingStats& operator += (TCpuAccountingStats& lhs, const TCpuAccountingStats& rhs)
{
    lhs.set_user_time(lhs.user_time() + rhs.user_time());
    lhs.set_system_time(lhs.system_time() + rhs.system_time());
    return lhs;
}

TCpuAccountingStats operator + (const TCpuAccountingStats& lhs, const TCpuAccountingStats& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TCpuAccountingStats& operator -= (TCpuAccountingStats& lhs, const TCpuAccountingStats& rhs)
{
    lhs.set_user_time(lhs.user_time() - rhs.user_time());
    lhs.set_system_time(lhs.system_time() - rhs.system_time());
    return lhs;
}

TCpuAccountingStats operator - (const TCpuAccountingStats& lhs, const TCpuAccountingStats& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

void Serialize(const TCpuAccountingStats& statistics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("user_time").Value(statistics.user_time())
            .Item("system_time").Value(statistics.system_time())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TBlockIOStats& operator += (TBlockIOStats& lhs, const TBlockIOStats& rhs)
{
    lhs.set_total_sectors(lhs.total_sectors() + rhs.total_sectors());
    lhs.set_bytes_read(lhs.bytes_read() + rhs.bytes_read());
    lhs.set_bytes_written(lhs.bytes_written() + rhs.bytes_written());
    return lhs;
}

TBlockIOStats operator + (const TBlockIOStats& lhs, const TBlockIOStats& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TBlockIOStats& operator -= (TBlockIOStats& lhs, const TBlockIOStats& rhs)
{
    lhs.set_total_sectors(lhs.total_sectors() - rhs.total_sectors());
    lhs.set_bytes_read(lhs.bytes_read() - rhs.bytes_read());
    lhs.set_bytes_written(lhs.bytes_written() - rhs.bytes_written());
    return lhs;
}

TBlockIOStats operator - (const TBlockIOStats& lhs, const TBlockIOStats& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
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
