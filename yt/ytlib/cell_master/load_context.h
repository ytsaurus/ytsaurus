#pragma once

#include "public.h"

// TODO: remove
#include <ytlib/object_server/id.h>
#include <ytlib/transaction_server/public.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/cypress/public.h>

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

template <>
NChunkServer::TChunkList* TLoadContext::Get(const NObjectServer::TObjectId& id) const;

template <>
NChunkServer::TChunk* TLoadContext::Get(const NObjectServer::TObjectId& id) const;

template <>
NChunkServer::TJob* TLoadContext::Get(const NObjectServer::TObjectId& id) const;

template <>
NCypress::TLock* TLoadContext::Get(const NObjectServer::TObjectId& id) const;

////////////////////////////////////////////////////////////////////////////////

void SaveObject(TOutputStream* output, const NObjectServer::TObjectWithIdBase* object);

template <class T>
void LoadObject(TInputStream* input, T*& object, const TLoadContext& context);

template <class T>
void SaveObjects(TOutputStream* output, const T& objects);

template <class T>
void LoadObjects(TInputStream* input, std::vector<T*>& objects, const TLoadContext& context);

template <class T>
void LoadObjects(TInputStream* input, yhash_set<T*>& objects, const TLoadContext& context);

template <>
void SaveObjects(TOutputStream* output, const std::vector<NChunkServer::TChunkTreeRef>& objects);
void LoadObjects(TInputStream* input, std::vector<NChunkServer::TChunkTreeRef>& objects, const TLoadContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

#define LOAD_CONTEXT_INL_H_
#include "load_context-inl.h"
#undef LOAD_CONTEXT_INL_H_
