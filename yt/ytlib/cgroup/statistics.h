#pragma once

#include <ytlib/cgroup/statistics.pb.h>
#include <core/yson/public.h>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

TCpuAccountingStatistics& operator += (TCpuAccountingStatistics& lhs, const TCpuAccountingStatistics& rhs);
TCpuAccountingStatistics operator + (TCpuAccountingStatistics lhs, const TCpuAccountingStatistics& rhs);

TCpuAccountingStatistics& operator -= (TCpuAccountingStatistics& lhs, const TCpuAccountingStatistics& rhs);
TCpuAccountingStatistics operator - (TCpuAccountingStatistics lhs, const TCpuAccountingStatistics& rhs);

void Serialize(const TCpuAccountingStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

TBlockIOStatistics& operator += (TBlockIOStatistics& lhs, const TBlockIOStatistics& rhs);
TBlockIOStatistics operator + (TBlockIOStatistics lhs, const TBlockIOStatistics& rhs);

TBlockIOStatistics& operator -= (TBlockIOStatistics& lhs, const TBlockIOStatistics& rhs);
TBlockIOStatistics operator - (TBlockIOStatistics lhs, const TBlockIOStatistics& rhs);

void Serialize(const TBlockIOStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
