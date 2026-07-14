#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Runs |callback|, tagging any thrown error with |what| and |key| and preserving the original.
//! Zero-cost when nothing throws; fiber cancellation still propagates.
template <class TCallback>
void TagErrorWithKey(TStringBuf what, const TKey& key, const TCallback& callback)
{
    try {
        callback();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to process %v", what)
            << TErrorAttribute("key", key)
            << TError(ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
