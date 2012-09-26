#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

#include <ytlib/cypress_client/public.h>

#include <server/cypress_server/public.h>

#include <server/chunk_server/public.h>

#include <server/object_server/object_detail.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETransactionState,
    (Active)
    (Committed)
    (Aborted)
);

class TTransaction
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYVAL_RW_PROPERTY(ETransactionState, State);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TTransaction*>, NestedTransactions);
    DEFINE_BYVAL_RW_PROPERTY(TTransaction*, Parent);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, StartTime);

    // Object Manager stuff
    DEFINE_BYREF_RW_PROPERTY(yhash_set<NObjectServer::TObjectId>, CreatedObjectIds);

    // Cypress stuff
    DEFINE_BYREF_RW_PROPERTY(std::vector<NCypressServer::ICypressNode*>, LockedNodes);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NCypressServer::ICypressNode*>, BranchedNodes);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NCypressServer::ICypressNode*>, CreatedNodes);

public:
    explicit TTransaction(const TTransactionId& id);

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

    bool IsActive() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
