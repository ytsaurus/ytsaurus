#pragma once

#include "public.h"
#include "automaton.h"

#include <core/misc/property.h>
#include <core/misc/small_vector.h>

#include <server/hydra/composite_automaton.h>

#include <server/node_tracker_server/public.h>

#include <server/object_server/public.h>

#include <server/transaction_server/public.h>

#include <server/chunk_server/public.h>

#include <server/cypress_server/public.h>

#include <server/security_server/public.h>

#include <server/table_server/public.h>

#include <server/tablet_server/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

template <>
NObjectServer::TObjectBase* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NTransactionServer::TTransaction* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NCypressServer::TLock* TLoadContext::Get(const NCypressClient::TLockId& id) const;

template <>
NChunkServer::TChunkList* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NChunkServer::TChunk* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NChunkServer::TJob* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NChunkServer::TChunkOwnerBase* TLoadContext::Get(const NCypressClient::TVersionedNodeId& id) const;

template <>
NCypressServer::TCypressNodeBase* TLoadContext::Get(const NCypressClient::TNodeId& id) const;

template <>
NCypressServer::TCypressNodeBase* TLoadContext::Get(const NCypressClient::TVersionedNodeId& id) const;

template <>
NSecurityServer::TAccount* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NNodeTrackerServer::TNode* TLoadContext::Get(NNodeTrackerServer::TNodeId id) const;

template <>
NSecurityServer::TSubject* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NSecurityServer::TUser* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NSecurityServer::TGroup* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NTableServer::TTableNode* TLoadContext::Get(const NCypressClient::TVersionedNodeId& id) const;

template <>
NTabletServer::TTabletCell* TLoadContext::Get(const NTabletServer::TTabletCellId& id) const;

template <>
NTabletServer::TTablet* TLoadContext::Get(const NTabletServer::TTabletId& id) const;

////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveObjectRef(TSaveContext& context, T object);

template <class T>
void LoadObjectRef(TLoadContext& context, T& object);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveObjectRefs(TSaveContext& context, const T& object);

template <class T>
void LoadObjectRefs(TLoadContext& context, T& object);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveNullableObjectRefs(TSaveContext& context, const std::unique_ptr<T>& objects);

template <class T>
void LoadNullableObjectRefs(TLoadContext& context, std::unique_ptr<T>& objects);

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): eliminate this hack when new serialization API is ready
template <class T>
void SaveObjectRef(TSaveContext& context, NChunkServer::TPtrWithIndex<T> value);
template <class T>
void LoadObjectRef(TLoadContext& context, NChunkServer::TPtrWithIndex<T>& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

#define SERIALIZATION_CONTEXT_INL_H_
#include "serialization_context-inl.h"
#undef SERIALIZATION_CONTEXT_INL_H_
