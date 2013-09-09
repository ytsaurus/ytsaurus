#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterConfig;
typedef TIntrusivePtr<TCellMasterConfig> TCellMasterConfigPtr;

class TMetaStateFacade;
typedef TIntrusivePtr<TMetaStateFacade> TMetaStateFacadePtr;

class TBootstrap;

class TLoadContext;
class TSaveContext;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
