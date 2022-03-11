#pragma once

#include <yt/yt/core/misc/ref_counted.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TAlienCellDescriptorLite;
struct TAlienPeerDescriptor;
struct TAlienCellDescriptor;

DECLARE_REFCOUNTED_STRUCT(IChaosCellDirectorySynchronizer)
DECLARE_REFCOUNTED_CLASS(TChaosCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
