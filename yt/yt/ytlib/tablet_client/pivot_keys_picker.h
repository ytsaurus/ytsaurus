#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

std::vector<NTableClient::TLegacyOwningKey> PickPivotKeysWithSlicing(
    const NYPath::TYPath& path,
    int tabletCount,
    const NApi::TReshardTableOptions& options,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& Logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
