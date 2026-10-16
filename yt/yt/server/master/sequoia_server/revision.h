#pragma once

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/client/hydra/public.h>

#include <util/generic/function_ref.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaRevisionPrepare
{
    std::function<void(NObjectClient::TVersionedObjectId, NObjectServer::EModificationType)> PrepareNodeModification;

    // NB: the only case when this revision is needed is scion creation. Don't
    // use it without deep understanding what are you doing.
    NHydra::TRevision NonMonotonicRevision;
};

struct TSequoiaRevisionCommit
{
    NHydra::TRevision Revision;
};

struct TSequoiaRevisionDisabled
{ };

// NB: prepare timestamp is unreliable since it is not guaranteed to be
// monotonic. There is the single cursed case where Sequoia revision has to be
// used on prapre phase. On other cases object revision change in prepare phase
// of transactions means either late prepare or object creation.
using TSequoiaRevision = std::variant<
    TSequoiaRevisionPrepare,
    TSequoiaRevisionCommit,
    TSequoiaRevisionDisabled>;

std::optional<TSequoiaRevision> GetCurrentSequoiaRevision();

struct TSequoiaRevisionGuard
{
    explicit TSequoiaRevisionGuard(TSequoiaRevision revision);
    ~TSequoiaRevisionGuard();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
