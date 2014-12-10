#pragma once

#include "public.h"
#include "automaton.h"

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
NNodeTrackerServer::TRack* TLoadContext::Get(const NNodeTrackerServer::TRackId& id) const;

template <>
NSecurityServer::TSubject* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NSecurityServer::TUser* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NSecurityServer::TGroup* TLoadContext::Get(const NObjectClient::TObjectId& id) const;

template <>
NTableServer::TTableNode* TLoadContext::Get(const NCypressClient::TVersionedNodeId& id) const;

template <>
NTabletServer::TTabletCell* TLoadContext::Get(const NTabletClient::TTabletCellId& id) const;

template <>
NTabletServer::TTablet* TLoadContext::Get(const NTabletClient::TTabletId& id) const;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
     
#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
