#include "updates_tracker.h"

#include "format_helpers.h"

#include <Poco/Exception.h>

#include <util/generic/maybe.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TUpdatesTracker
    : public IUpdatesTracker
{
private:
    NInterop::IStoragePtr Storage;
    NInterop::IAuthorizationTokenPtr Token;
    std::string Path;

    TMaybe<NInterop::TRevision> FixedRevision;

public:
    TUpdatesTracker(
        NInterop::IStoragePtr storage,
        NInterop::IAuthorizationTokenPtr token,
        std::string path)
        : Storage(std::move(storage))
        , Token(std::move(token))
        , Path(std::move(path))
    {}

    bool IsModified() const override;
    void FixCurrentVersion() override;

private:
    TMaybe<NInterop::TRevision> GetCurrentRevision() const;
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

TMaybe<NInterop::TRevision> TUpdatesTracker::GetCurrentRevision() const
{
    return Storage->GetObjectRevision(*Token, ToString(Path), /*throughCache=*/ true);
}

////////////////////////////////////////////////////////////////////////////////

IUpdatesTrackerPtr CreateUpdatesTracker(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr token,
    const std::string& path)
{
    return std::make_unique<TUpdatesTracker>(
        std::move(storage),
        std::move(token),
        path);
}

} // namespace NClickHouse
} // namespace NYT
