#pragma once

#include "public.h"

#include <server/transaction_server/public.h>
#include <server/chunk_server/public.h>
#include <server/cypress_server/public.h>

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
    T* Get(const NObjectClient::TObjectId& id) const;

    template <class T>
    T* Get(const NObjectClient::TVersionedObjectId& id) const;
};

template <>
NTransactionServer::TTransaction* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NChunkServer::TChunkList* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NChunkServer::TChunk* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NChunkServer::TJob* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NCypressServer::ICypressNode* TLoadContext::Get(const NObjectClient::TVersionedObjectId& id) const;

////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveObjectRef(TOutputStream* output, T object);

template <class T>
void LoadObjectRef(TInputStream* input, T& object, const TLoadContext& context);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveObjectRefs(TOutputStream* output, const T& object);

template <class T>
void LoadObjectRefs(TInputStream* input, T& object, const TLoadContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

#define LOAD_CONTEXT_INL_H_
#include "load_context-inl.h"
#undef LOAD_CONTEXT_INL_H_
