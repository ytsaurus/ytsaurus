#include "updates_tracker.h"

#include "format_helpers.h"

#include <Poco/Exception.h>

#include <yt/server/clickhouse_server/objects.h>
#include <yt/server/clickhouse_server/query_context.h>

#include <util/generic/maybe.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TUpdatesTracker
    : public IUpdatesTracker
{
private:
    TQueryContext* Storage;
    IAuthorizationTokenPtr Token;
    std::string Path;

    std::optional<TRevision> FixedRevision;

public:
    TUpdatesTracker(
        TQueryContext* storage,
        IAuthorizationTokenPtr token,
        std::string path)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , Path(std::move(path))
    {}

    bool IsModified() const override;
    void FixCurrentVersion() override;

private:
    std::optional<TRevision> GetCurrentRevision() const;
};

////////////////////////////////////////////////////////////////////////////////

bool TUpdatesTracker::IsModified() const
{
    auto currentRevision = GetCurrentRevision();
    if (!currentRevision) {
        return false;
    }
    return !FixedRevision || *currentRevision != *FixedRevision;
}

void TUpdatesTracker::FixCurrentVersion()
{
    auto currentRevision = GetCurrentRevision();
    if (!currentRevision) {
        throw Poco::Exception(
            "Cannot fix revision of " + Quoted(Path) + ": object not found");
    }
    FixedRevision = currentRevision;
}

std::optional<TRevision> TUpdatesTracker::GetCurrentRevision() const
{
    return Storage->GetObjectRevision(*Token, ToString(Path), /*throughCache=*/ true);
}

////////////////////////////////////////////////////////////////////////////////

IUpdatesTrackerPtr CreateUpdatesTracker(
    TQueryContext* storage,
    IAuthorizationTokenPtr token,
    const std::string& path)
{
    return std::make_unique<TUpdatesTracker>(
        std::move(storage),
        std::move(token),
        path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
