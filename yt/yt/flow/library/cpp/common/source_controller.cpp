#include "source_controller.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TExtendedSourcePartitionStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("partition_status", &TThis::PartitionStatus);
    registrar.Parameter("partition_state", &TThis::PartitionState);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
