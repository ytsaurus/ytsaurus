#pragma once

#include "private.h"

#include <yp/server/objects/public.h>

#include <yp/server/master/public.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TLabelFilterCacheBase
{
protected:
    const NMaster::TYTConnectorPtr YTConnector_;
    NObjects::IObjectTypeHandler* const TypeHandler_;
    const std::vector<TObject*> AllObjects_;

    TLabelFilterCacheBase(
        NMaster::TYTConnectorPtr ytConnector,
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
        NMaster::TYTConnectorPtr ytConnector,
        NObjects::IObjectTypeHandler* typeHandler,
        const std::vector<T*>& allObjects);

    const TErrorOr<std::vector<T*>>& GetFilteredObjects(const TString& query);

private:
    THashMap<TString, TErrorOr<std::vector<T*>>> QueryToObjects_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP


#define LABEL_FILTER_CACHE_INL_H_
#include "label_filter_cache-inl.h"
#undef LABEL_FILTER_CACHE_INL_H_