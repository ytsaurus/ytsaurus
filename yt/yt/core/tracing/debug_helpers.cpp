#include "debug_helpers.h"

#include <yt/yt/core/concurrency/propagating_storage.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

TTraceContext* RetrieveTraceContextFromPropStorage(NConcurrency::TPropagatingStorage* storage)
{
    auto result = storage->Find<TTraceContextPtr>();
    return result ? result->Get() : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
