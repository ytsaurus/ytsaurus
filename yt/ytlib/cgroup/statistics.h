#pragma once

#include <ytlib/cgroup/statistics.pb.h>
#include <core/yson/public.h>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

TCpuAccountingStats& operator += (TCpuAccountingStats& lhs, const TCpuAccountingStats& rhs);
TCpuAccountingStats operator + (TCpuAccountingStats lhs, const TCpuAccountingStats& rhs);

TCpuAccountingStats& operator -= (TCpuAccountingStats& lhs, const TCpuAccountingStats& rhs);
TCpuAccountingStats operator - (TCpuAccountingStats lhs, const TCpuAccountingStats& rhs);

void Serialize(const TCpuAccountingStats& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

TBlockIOStats& operator += (TBlockIOStats& lhs, const TBlockIOStats& rhs);
TBlockIOStats operator + (TBlockIOStats lhs, const TBlockIOStats& rhs);

TBlockIOStats& operator -= (TBlockIOStats& lhs, const TBlockIOStats& rhs);
TBlockIOStats operator - (TBlockIOStats lhs, const TBlockIOStats& rhs);

void Serialize(const TBlockIOStats& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
