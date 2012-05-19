#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterConfig;
typedef TIntrusivePtr<TCellMasterConfig> TCellMasterConfigPtr;

class TWorldInitializer;
typedef TIntrusivePtr<TWorldInitializer> TWorldInitializerPtr;

class TBootstrap;

class TLoadContext;

////////////////////////////////////////////////////////////////////////////////
            
const int StateThreadQueueCount = 2;

DECLARE_ENUM(EStateThreadQueue,
    (Default)
    (ChunkRefresh)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
