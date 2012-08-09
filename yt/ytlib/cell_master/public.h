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
            
} // namespace NCellMaster
} // namespace NYT
