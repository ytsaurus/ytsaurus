#pragma once
#ifndef OBJECT_FILTER_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include object_filter_cache.h"
// For the sake of sane code completion.
#include "object_filter_cache.h"
#endif

#include "object_filter_evaluator.h"

#include <yp/server/lib/objects/object_filter.h>

#include <yt/core/misc/error.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TObjectFilterCache<T>::TObjectFilterCache(
    IObjectFilterEvaluatorPtr evaluator,
    const std::vector<T*>& objects)
    : Evaluator_(std::move(evaluator))
    , UntypedObjects_(objects.begin(), objects.end())
{ }

template <class T>
const TErrorOr<std::vector<T*>>& TObjectFilterCache<T>::Get(
    const NObjects::TObjectFilter& filter)
{
    const auto& query = filter.Query;
    auto it = QueryToResult_.find(query);
    if (it == QueryToResult_.end()) {
        auto objectsOrError = Evaluator_->Evaluate(
            T::Type,
            UntypedObjects_,
            filter);
        TErrorOr<std::vector<T*>> typedObjectsOrError;
        if (objectsOrError.IsOK()) {
            std::vector<T*> typedObjects;
            const auto& objects = objectsOrError.Value();
            typedObjects.reserve(objects.size());
            for (auto* object : objects) {
                typedObjects.push_back(object->template As<T>());
            }
            typedObjectsOrError = TErrorOr<std::vector<T*>>(std::move(typedObjects));
        } else {
            typedObjectsOrError = TError(objectsOrError);
        }
        bool inserted;
        std::tie(it, inserted) = QueryToResult_.emplace(query, std::move(typedObjectsOrError));
        YT_VERIFY(inserted);
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
