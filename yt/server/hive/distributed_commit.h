#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/transaction_client/public.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TDistributedCommit
{
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, TransactionId);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TCellGuid>, ParticipantCellGuids);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TCellGuid>, PreparedParticipantCellGuids);

public:
    explicit TDistributedCommit(const TTransactionId& transactionId);
    TDistributedCommit(
        const TTransactionId& transactionId,
        const std::vector<TCellGuid>& participantCellGuids);

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
