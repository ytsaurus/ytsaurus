#include "source_context.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TSourceContextPtr CreateTestSourceContext(IInvokerPtr serializedInvoker)
{
    auto context = New<TSourceContext>();
    context->SerializedInvoker = std::move(serializedInvoker);
    context->Partition = NYTree::ConvertTo<TPartitionPtr>(NYson::TYsonString(TStringBuf(R"""({
        "partition_id" = "48946f5e-ac1b2be7-4babe692-8af11700";
        "computation_id" = "48946f5e-ac1b2be7-4babe692-8af11700";
        "parameters" = {};
        "state_epoch" = 0;
        "state_timestamp" = "2020-01-01T00:00:00Z";
    })""")));
    context->Logger = NLogging::TLogger("Test");
    context->StatusProfiler = CreateSyncStatusProfiler();
    return context;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
