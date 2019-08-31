#pragma once

#include "public.h"

#include <yp/server/lib/objects/public.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TObjectFilterCache
{
public:
    TObjectFilterCache(
        IObjectFilterEvaluatorPtr evaluator,
        const std::vector<T*>& objects);

    const TErrorOr<std::vector<T*>>& Get(const NObjects::TObjectFilter& filter);

private:
    const IObjectFilterEvaluatorPtr Evaluator_;
    const std::vector<TObject*> UntypedObjects_;

    THashMap<TString, TErrorOr<std::vector<T*>>> QueryToResult_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster

#define OBJECT_FILTER_CACHE_INL_H_
#include "object_filter_cache-inl.h"
#undef OBJECT_FILTER_CACHE_INL_H_
