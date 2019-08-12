#pragma once

#include "private.h"

#include <yp/server/objects/public.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TLabelFilterCacheBase
{
protected:
    NObjects::IObjectTypeHandler* const TypeHandler_;
    const std::vector<TObject*> AllObjects_;

    TLabelFilterCacheBase(
        NObjects::IObjectTypeHandler* const typeHandler,
        std::vector<TObject*> allObjects);

    TErrorOr<std::vector<TObject*>> DoGetFilteredObjects(const TString& query);
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TLabelFilterCache
    : public TLabelFilterCacheBase
{
public:
    TLabelFilterCache(
        NObjects::IObjectTypeHandler* typeHandler,
        const std::vector<T*>& allObjects);

    const TErrorOr<std::vector<T*>>& GetFilteredObjects(const TString& query);

private:
    THashMap<TString, TErrorOr<std::vector<T*>>> QueryToObjects_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler


#define LABEL_FILTER_CACHE_INL_H_
#include "label_filter_cache-inl.h"
#undef LABEL_FILTER_CACHE_INL_H
