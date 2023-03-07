#include "revision_tracker.h"

#include <yt/client/api/client.h>

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NApi;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TRevisionTracker::TRevisionTracker(TYPath path, IClientPtr client)
    : Path_(std::move(path))
    , Client_(std::move(client))
{ }

bool TRevisionTracker::HasRevisionChanged() const
{
    if (Revision_ == NHydra::NullRevision) {
        return true;
    }
    auto currentRevision = GetCurrentRevision();
    if (currentRevision == NHydra::NullRevision) {
        // We do not want to lose state of the dictionary for as long as possible.
        return false;
    }
    YT_VERIFY(currentRevision >= Revision_);
    return currentRevision != Revision_;
}

void TRevisionTracker::FixCurrentRevision()
{
    Revision_ = GetCurrentRevision();
}

NHydra::TRevision TRevisionTracker::GetRevision() const
{
    return Revision_;
}

NHydra::TRevision TRevisionTracker::GetCurrentRevision() const
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    auto asyncResult = Client_->GetNode(Path_ + "/@revision", options);
    auto ysonOrError = WaitFor(asyncResult);
    if (!ysonOrError.IsOK()) {
        if (ysonOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return NHydra::NullRevision;
        }
        ysonOrError.ThrowOnError();
    }
    return ConvertTo<NHydra::TRevision>(ysonOrError.Value());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
