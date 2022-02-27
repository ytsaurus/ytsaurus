#include "helpers.h"

#include "dynamic_state.h"

#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NQueueAgent {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TErrorOr<EQueueFamily> DeduceQueueFamily(const TQueueTableRow& row)
{
    if (row.ObjectType == EObjectType::Table) {
        // NB: Dynamic and Sorted or optionals.
        if (row.Dynamic == true && row.Sorted == false) {
            return EQueueFamily::OrderedDynamicTable;
        }
        return TError("Only ordered dynamic tables are supported as queues");
    }

    return TError("Invalid queue object type %Qlv", row.ObjectType);
}

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr AssertNativeClient(const NApi::IClientPtr& client)
{
    auto nativeClient = MakeStrong(dynamic_cast<NApi::NNative::IClient*>(client.Get()));
    YT_VERIFY(nativeClient);
    return nativeClient;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
