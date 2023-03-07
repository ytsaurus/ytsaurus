#pragma once

#ifndef LIST_OPERATIONS_INL_H
#error "Direct inclusion of this file is not allowed, include list_operations.h"
// For the sake of sane code completion.
#include "list_operations.h"
#endif

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

template <typename TFunction>
void TListOperationsFilter::ForEachOperationImmutable(TFunction function) const
{
    for (int i = 0; i < static_cast<int>(LightOperations_.size()); ++i) {
        function(i, LightOperations_[i]);
    }
}

template <typename TFunction>
void TListOperationsFilter::ForEachOperationMutable(TFunction function)
{
    for (int i = 0; i < static_cast<int>(LightOperations_.size()); ++i) {
        function(i, LightOperations_[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
