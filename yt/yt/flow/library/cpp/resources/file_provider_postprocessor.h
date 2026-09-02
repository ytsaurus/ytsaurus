#pragma once

#include <yt/yt/flow/library/cpp/common/file_provider.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void PostprocessFileProvider(
    const TFileProviderId& providerId,
    const TFileProviderRevisionPtr& revision,
    const TFileProviderSpecPtr& providerSpec,
    NFileStorage::IFileStorageObjectPtr inputObject,
    const std::string& resultPath,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
