#include "helpers.h"

#include "state.h"

namespace NYT::NQueueAgent {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TErrorOr<EQueueType> DeduceQueueType(const TQueueTableRow& row)
{
    if (!row.ObjectType) {
        return TError("Queue object type is not known yet");
    }

    if (row.ObjectType == EObjectType::Table) {
        // NB: Dynamic and Sorted or optionals.
        if (row.Dynamic == true && row.Sorted == false) {
            return EQueueType::OrderedDynamicTable;
        }
        return TError("Only ordered dynamic tables are supported as queues");
    }

    return TError("Invalid queue object type %Qlv", row.ObjectType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
