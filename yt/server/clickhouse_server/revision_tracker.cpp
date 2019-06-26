#include "revision_tracker.h"

#include "format_helpers.h"

#include "objects.h"
#include "query_context.h"

namespace NYT::NClickHouseServer {

using namespace NYPath;
using namespace NApi;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TRevisionTracker::TRevisionTracker(TYPath path, IClientPtr client)
    : Path_(std::move(path))
    , Client_(std::move(client))
{ }

bool TRevisionTracker::HasRevisionChanged() const
{
    if (!Revision_) {
        return true;
    }
    auto currentRevision = GetCurrentRevision();
    if (!currentRevision) {
        // We do not want to lose state of the dictionary for as long as possible.
        return false;
    }
    YT_VERIFY(*currentRevision >= *Revision_);
    return *currentRevision != *Revision_;
}

void TRevisionTracker::FixCurrentRevision()
{
    Revision_ = GetCurrentRevision();
}

std::optional<ui64> TRevisionTracker::GetRevision() const
{
    return Revision_;
}

std::optional<ui64> TRevisionTracker::GetCurrentRevision() const
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    auto asyncResult = Client_->GetNode(Path_ + "/@revision", options);
    auto ysonOrError = WaitFor(asyncResult);
    if (!ysonOrError.IsOK()) {
        if (ysonOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return std::nullopt;
        }
        ysonOrError.ThrowOnError();
    }
    return ConvertTo<ui64>(ysonOrError.Value());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
