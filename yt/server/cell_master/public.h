#pragma once

#include <core/misc/common.h>
// TODO(babenko): replace by public.h
#include <core/misc/serialize.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMasterCellConfig)
DECLARE_REFCOUNTED_CLASS(TCellMasterConfig)

DECLARE_REFCOUNTED_CLASS(TMasterAutomaton)
DECLARE_REFCOUNTED_CLASS(TMasterAutomatonPart)
DECLARE_REFCOUNTED_CLASS(TMetaStateFacade)
DECLARE_REFCOUNTED_CLASS(TWorldInitializer)

class TBootstrap;

class TLoadContext;
class TSaveContext;
typedef TCustomPersistenceContext<TSaveContext, TLoadContext> TPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
