#include "medium_directory_manager.h"

#include "bootstrap.h"

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
    YT_LOG_DEBUG("Waiting for at least one medium directory synchronization since startup");
    WaitFor(mediumDirectorySynchronizer
        ->RecentSync())
        .ThrowOnError();
    YT_LOG_DEBUG("Medium directory synchronization finished");
    return connection->GetMediumDirectory();
}

void TMediumDirectoryManager::UpdateMediumDirectory(const NProto::TMediumDirectory& mediumDirectory)
{
    auto newMediumDirectory = New<TMediumDirectory>();
    newMediumDirectory->LoadFrom(mediumDirectory);
    MediumDirectory_.Store(std::move(newMediumDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
