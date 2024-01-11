#pragma once

#include <yt/yt/server/lib/lsm/store.h>

#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

extern NLogging::TLogger Logger;

////////////////////////////////////////////////////////////////////////////////

class TStore;
class TPartition;
class TStoreManager;
DECLARE_REFCOUNTED_CLASS(TTablet)
DECLARE_REFCOUNTED_STRUCT(IActionQueue)
DECLARE_REFCOUNTED_STRUCT(TWriterSpec)
DECLARE_REFCOUNTED_STRUCT(TMountConfigOptimizerConfig)
DECLARE_REFCOUNTED_STRUCT(TInitialDataDescription)
DECLARE_REFCOUNTED_STRUCT(TLsmSimulatorConfig)
DECLARE_REFCOUNTED_STRUCT(ILsmSimulator)
DECLARE_REFCOUNTED_CLASS(TStructuredLogger)

using TNativeKey = NTableClient::TUnversionedOwningRow;

// Used for ID generation.
extern int EpochIndex;

////////////////////////////////////////////////////////////////////////////////

#undef Cerr
#define Cerr NLogging::TLogManager::Get()->Synchronize(); (::NPrivate::StdErrStream())

} // namespace NYT::NLsm::NTesting
