#pragma once

#include <core/misc/common.h>
// TODO(babenko): replace by public.h
#include <core/misc/serialize.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMasterCellConfig;
typedef TIntrusivePtr<TMasterCellConfig> TMasterCellConfigPtr;

class TCellMasterConfig;
typedef TIntrusivePtr<TCellMasterConfig> TCellMasterConfigPtr;

class TMasterAutomaton;
typedef TIntrusivePtr<TMasterAutomaton> TMasterAutomatonPtr;

class TMasterAutomatonPart;

class TMetaStateFacade;
typedef TIntrusivePtr<TMetaStateFacade> TMetaStateFacadePtr;

class TWorldInitializer;
typedef TIntrusivePtr<TWorldInitializer> TWorldInitializerPtr;

class TBootstrap;

class TLoadContext;
class TSaveContext;
typedef TCustomPersistenceContext<TSaveContext, TLoadContext> TPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
