#include "updates_tracker.h"

#include "format_helpers.h"

//#include <Poco/Exception.h>

#include <yt/server/clickhouse_server/native/objects.h>
#include <yt/server/clickhouse_server/native/storage.h>

#include <util/generic/maybe.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

class TUpdatesTracker
    : public IUpdatesTracker
{
private:
    NNative::IStoragePtr Storage;
    NNative::IAuthorizationTokenPtr Token;
    std::string Path;

    TMaybe<NNative::TRevision> FixedRevision;

public:
    TUpdatesTracker(
        NNative::IStoragePtr storage,
        NNative::IAuthorizationTokenPtr token,
        std::string path)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , Path(std::move(path))
    {}

    bool IsModified() const override;
    void FixCurrentVersion() override;

private:
    TMaybe<NNative::TRevision> GetCurrentRevision() const;
};

////////////////////////////////////////////////////////////////////////////////

bool TUpdatesTracker::IsModified() const
{
    auto currentRevision = GetCurrentRevision();
    if (!currentRevision.Defined()) {
        return false;
    }
    return !FixedRevision.Defined() || *currentRevision != *FixedRevision;
}

void TUpdatesTracker::FixCurrentVersion()
{
    auto currentRevision = GetCurrentRevision();
    if (!currentRevision.Defined()) {
        throw Poco::Exception(
            "Cannot fix revision of " + Quoted(Path) + ": object not found");
    }
    FixedRevision = currentRevision;
}

TMaybe<NNative::TRevision> TUpdatesTracker::GetCurrentRevision() const
{
    return Storage->GetObjectRevision(*Token, ToString(Path), /*throughCache=*/ true);
}

////////////////////////////////////////////////////////////////////////////////

IUpdatesTrackerPtr CreateUpdatesTracker(
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr token,
    const std::string& path)
{
    return std::make_unique<TUpdatesTracker>(
        std::move(storage),
        std::move(token),
        path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
