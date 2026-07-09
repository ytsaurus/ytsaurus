#include "job_spec.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

void TJobSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("computation_spec", &TThis::ComputationSpec);
    registrar.Parameter("extended_computation_spec", &TThis::ExtendedComputationSpec);

    registrar.Parameter("job", &TThis::Job);
    registrar.Parameter("partition", &TThis::Partition);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicJobSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("dynamic_computation_spec", &TThis::DynamicComputationSpec);
    registrar.Parameter("dynamic_computation_partition_spec", &TThis::DynamicComputationPartitionSpec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
