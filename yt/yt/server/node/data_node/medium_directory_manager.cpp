#include "medium_directory_manager.h"

#include "bootstrap.h"
#include "chunk_store.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/medium_directory.h>
#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NConcurrency;

namespace NProto = NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TMediumDirectoryManager::TMediumDirectoryManager(
    IBootstrap* bootstrap,
    const NLogging::TLogger& logger)
    : Bootstrap_(bootstrap)
    , Logger(logger)
{ }

TMediumDirectoryPtr TMediumDirectoryManager::GetMediumDirectory() const
{
    if (auto mediumDirectory = MediumDirectory_.Acquire()) {
        return mediumDirectory;
    }

    const auto& connection = Bootstrap_->GetClient()->GetNativeConnection();
    const auto& mediumDirectorySynchronizer = connection->GetMediumDirectorySynchronizer();
    YT_TLOG_DEBUG("Waiting for at least one medium directory synchronization since startup");
    WaitFor(mediumDirectorySynchronizer
        ->RecentSync())
        .ThrowOnError();
    YT_TLOG_DEBUG("Medium directory synchronization finished");
    return connection->GetMediumDirectory();
}

void TMediumDirectoryManager::UpdateMediumDirectory(const NProto::TMediumDirectory& mediumDirectory)
{
    auto newMediumDirectory = New<TMediumDirectory>();
    newMediumDirectory->LoadFrom(mediumDirectory);
    MediumDirectory_.Store(std::move(newMediumDirectory));
}

void TMediumDirectoryManager::PopulateMediumDirectoryFilter(NProto::TMediumDirectoryFilter* filter) const
{
    THashSet<std::string> mediumNames;
    THashSet<int> mediumIndexes;
    for (const auto& location : Bootstrap_->GetChunkStore()->Locations()) {
        // Include the configured medium so clearing an override can restore it.
        if (const auto& mediumName = location->GetStaticConfig()->MediumName; mediumNames.insert(mediumName).second) {
            filter->add_statically_initialized_media(mediumName);
        }
        if (auto mediumIndex = location->GetMediumIndex(); mediumIndexes.insert(mediumIndex).second) {
            filter->add_applied_medium_indexes(mediumIndex);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
