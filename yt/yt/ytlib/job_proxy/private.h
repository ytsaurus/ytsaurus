#pragma once

#include <yt/core/logging/log.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

extern NLogging::TLogger JobProxyClientLogger;

TString GetDefaultJobsMetaContainerName();

TString GetSlotMetaContainerName(int slotIndex);
TString GetFullSlotMetaContainerName(const TString& jobsMetaName, int slotIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
