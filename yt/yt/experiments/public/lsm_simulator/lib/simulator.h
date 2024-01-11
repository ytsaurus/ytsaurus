#pragma once

#include "config.h"

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

struct ILsmSimulator
    : public TRefCounted
{
    virtual TStoreManager* GetStoreManager() const = 0;

    virtual void Start() = 0;
    virtual bool IsWritingCompleted() const = 0;
    virtual int GetWriterIndex() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ILsmSimulator)

////////////////////////////////////////////////////////////////////////////////

ILsmSimulatorPtr CreateLsmSimulator(
    TLsmSimulatorConfigPtr config,
    TTableMountConfigPtr mountConfig,
    IActionQueuePtr actionQueue,
    TTabletPtr tablet = nullptr,
    TStructuredLoggerPtr structuredLogger = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
