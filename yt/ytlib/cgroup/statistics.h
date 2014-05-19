#pragma once

#include <ytlib/cgroup/statistics.pb.h>
#include <core/yson/public.h>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void Serialize(const TCpuAccountingStats& statistics, NYson::IYsonConsumer* consumer);

void Serialize(const TBlockIOStats& statistics, NYson::IYsonConsumer* consumer);

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
