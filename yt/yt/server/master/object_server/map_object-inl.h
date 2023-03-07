#pragma once
#ifndef MAP_OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include map_object.h"
// For the sake of sane code completion.
#include "map_object.h"
#endif

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject, class TFunctor>
void TraverseMapObjectSubtreeTopDown(TObject* object, TFunctor functor)
{
    functor(object);

    for (const auto& [key, child] : object->KeyToChild()) {
        TraverseMapObjectSubtreeTopDown(child, functor);
    }
}

template <class TObject, class TResult, class TFunctor>
TResult AccumulateOverMapObjectSubtree(TObject* object, TResult init, TFunctor functor)
{
    auto result = std::move(init);
    TraverseMapObjectSubtreeTopDown(object, [&] (auto* currentObject) {
        functor(currentObject, &result);
    });
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
