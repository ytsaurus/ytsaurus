#include "debug_helper.h"

#include "yt/yt/core/concurrency/propagating_storage.h"

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

TTraceContext* RetrieveTraceContextFromPropStorage(NConcurrency::TPropagatingStorage* storage)
{
    if (auto result = storage->TryGet<NTracing::TTraceContextPtr>()) {
        return result->Get();
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
