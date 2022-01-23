#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

TErrorOr<EQueueFamily> DeduceQueueFamily(const TQueueTableRow& row);

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr GetClusterClient(
    NHiveClient::TClusterDirectoryPtr clusterDirectory,
    const TString& clusterName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
