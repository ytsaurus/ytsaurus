#include "config_reloader.h"

#include "logging_helpers.h"
#include "format_helpers.h"
#include "type_helpers.h"

#include <Common/Exception.h>

#include <Poco/Logger.h>

#include <common/logger_useful.h>

#include <util/system/event.h>

#include <thread>

namespace DB {

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}   // namespace DB

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TConfigReloader
    : public IConfigReloader
{
private:
    IConfigRepositoryPtr Repository;
    std::string Name;
    TUpdateConfigHook UpdateConfig;

    Poco::Logger* Logger;

    IConfigPollerPtr Poller;

    TMaybe<NInterop::TRevision> KnownRevision;

    bool Started = false;
    TManualEvent StopRequested;
    std::thread PollerThread;

public:
    TConfigReloader(IConfigRepositoryPtr repository,
                    std::string name,
                    TUpdateConfigHook updateConfig);

    void Start() override;

    ~TConfigReloader();

    TConfigReloader(const TConfigReloader& that) = delete;
    TConfigReloader(TConfigReloader&& that) = delete;

private:
    void Stop();

    void RunInBackground();

    void MonitorUpdates();
    TMaybe<NInterop::TRevision> GetCurrentRevision() const;
    bool IsTargetUpdated(NInterop::TRevision& newRevision) const;
    bool TryReload();
    void UpdateKnownRevision(NInterop::TRevision newRevision);
};

////////////////////////////////////////////////////////////////////////////////

TConfigReloader::TConfigReloader(IConfigRepositoryPtr repository,
                                 std::string name,
                                 TUpdateConfigHook updateConfig)
    : Repository(std::move(repository))
    , Name(std::move(name))
    , UpdateConfig(std::move(updateConfig))
    , Logger(&Poco::Logger::get("ConfigReloader"))
    , Poller(Repository->CreatePoller(Name))
{
}

TConfigReloader::~TConfigReloader()
{
    Stop();
}

void TConfigReloader::Start()
{
    if (Started) {
        throw DB::Exception("Config reloader already started", DB::ErrorCodes::LOGICAL_ERROR);
    }
    PollerThread = std::thread(&TConfigReloader::RunInBackground, this);
    Started = true;
}

void TConfigReloader::Stop()
{
    if (!Started) {
        return;
    }
    StopRequested.Signal();
    PollerThread.join();
}

void TConfigReloader::RunInBackground()
{
    try {
        MonitorUpdates();
    } catch (...) {
        LOG_ERROR(Logger, "ConfigReloader(" << Name << ") failed: " << CurrentExceptionText());
        // TODO: Something bad happened, do we need to abort?
    }
}

void TConfigReloader::MonitorUpdates()
{
    static const TDuration POLL_FREQUENCY = TDuration::Seconds(5);

    LOG_INFO(Logger, "Start monitoring " << Quoted(Name));

    while (!StopRequested.WaitT(POLL_FREQUENCY)) {
        NInterop::TRevision newRevision;
        if (IsTargetUpdated(newRevision)) {
            LOG_DEBUG(Logger, "Found new revision of " << Quoted(Name) << ": " << newRevision);
            if (TryReload()) {
                UpdateKnownRevision(newRevision);
            }
        }
    }

    LOG_DEBUG(Logger, "Stop monitoring " << Quoted(Name));
}

bool TConfigReloader::TryReload()
{
    IConfigPtr config;
    try {
        config = Repository->Load(Name);
    } catch (...) {
        LOG_WARNING(Logger, "Failed to load config " << Quoted(Name) << ": " << CurrentExceptionText());
        return false;
    }

    if (!config) {
        LOG_WARNING(Logger, "Failed to load config " << Quoted(Name));
        return false;
    }

    try {
        UpdateConfig(config);
    } catch (...) {
        LOG_WARNING(Logger, "Failed to update config " << Quoted(Name) << ": " << CurrentExceptionText());
        return false;
    }

    return true;
}

TMaybe<NInterop::TRevision> TConfigReloader::GetCurrentRevision() const
{
    try {
        return Poller->GetRevision();
    } catch (...) {
        LOG_WARNING(Logger, "Cannot get revision of " << Quoted(Name) << ": " << CurrentExceptionText());
        return Nothing();
    }
}

bool TConfigReloader::IsTargetUpdated(NInterop::TRevision& newRevision) const
{
    const auto revision = GetCurrentRevision();
    if (revision.Defined() && (revision != KnownRevision)) {
        // TODO: Check monotonicity?
        newRevision = *revision;
        return true;
    }
    return false;
}

void TConfigReloader::UpdateKnownRevision(NInterop::TRevision newRevision)
{
    KnownRevision = newRevision;
}

////////////////////////////////////////////////////////////////////////////////

IConfigReloaderPtr CreateConfigReloader(
    IConfigRepositoryPtr repository,
    const std::string& name,
    TUpdateConfigHook updateConfig)
{
    return std::make_unique<TConfigReloader>(
        std::move(repository),
        name,
        std::move(updateConfig));
}

} // namespace NClickHouse
} // namespace NYT
