#pragma once
#ifndef LABEL_FILTER_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include label_filter_cache.h"
#endif

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TLabelFilterCache<T>::TLabelFilterCache(
    NMaster::TYTConnectorPtr ytConnector,
    NObjects::IObjectTypeHandler* typeHandler,
    const std::vector<T*>& allObjects)
    : TLabelFilterCacheBase(
        std::move(ytConnector),
        typeHandler,
        std::vector<TObject*>(allObjects.begin(), allObjects.end()))
{ }

template <class T>
const TErrorOr<std::vector<T*>>& TLabelFilterCache<T>::GetFilteredObjects(const TString& query)
{
    auto it = QueryToObjects_.find(query);
    if (it == QueryToObjects_.end()) {
        auto objectsOrError = DoGetFilteredObjects(query);
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
        std::tie(it, inserted) = QueryToObjects_.emplace(query, std::move(typedObjectsOrError));
        YCHECK(inserted);
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
