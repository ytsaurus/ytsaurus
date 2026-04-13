#pragma once

#include <yt/yt/client/object_client/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

// NB: All instances ot this class has to have static lifetime.
struct ISequoiaTransactionActionSequencer
{
    //! Returns priority of a given tx action.
    virtual int GetActionPriority(TStringBuf actionType) const = 0;

    virtual ~ISequoiaTransactionActionSequencer() = default;
};

struct TSequoiaTransactionRequestPriorities
{
    int DatalessLockRow = 0;
    int LockRow = 0;
    int WriteRow = 0;
    int DeleteRow = 0;
};

// Examples of feature flags that could be placed here:
//   - shared write locks for mirrored transactions;
//   - consistent inheritable attributes;
//   - transaction coordinator during mirrored transaction replicator.
struct TSequoiaTransactionFeatures
{ };

struct TSequoiaTransactionOptions
{
    const ISequoiaTransactionActionSequencer* TransactionActionSequencer = nullptr;
    std::optional<TSequoiaTransactionRequestPriorities> RequestPriorities = std::nullopt;
    std::vector<NObjectClient::TTransactionId> CypressPrerequisiteTransactionIds = {};
    bool SequenceTabletCommitSessions = false;
    bool EnableVerboseLogging = false;
    bool SuppressStronglyOrderedTransactionBarrier = false;
    TSequoiaTransactionFeatures Features = {};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
