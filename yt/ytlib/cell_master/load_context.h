#pragma once

#include "public.h"

#include <ytlib/object_server/id.h>
#include <ytlib/transaction_server/public.h>

#include <ytlib/misc/property.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
{
public:
    explicit TLoadContext(TBootstrap* bootstrap);

    DEFINE_BYVAL_RO_PROPERTY(TBootstrap*, Bootstrap);

    template <class T>
    T* Get(const NObjectServer::TObjectId& id) const;
};

template <>
NTransactionServer::TTransaction* TLoadContext::Get(const NObjectServer::TObjectId& id) const;

////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveObjects(TOutputStream* output, const T& objects);

template <class T>
void LoadObjects(TInputStream* input, std::vector<T*>& objects, const TLoadContext& context);

template <class T>
void LoadObjects(TInputStream* input, yhash_set<T*>& objects, const TLoadContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

#define LOAD_CONTEXT_INL_H_
#include "load_context-inl.h"
#undef LOAD_CONTEXT_INL_H_
