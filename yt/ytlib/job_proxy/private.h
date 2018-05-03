#pragma once

#include <yt/core/logging/log.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

extern NLogging::TLogger JobProxyClientLogger;

TString GetDefaultJobsMetaContainerName();

TString GetSlotMetaContainerName(int slotIndex);
TString GetFullSlotMetaContainerName(const TString& jobsMetaName, int slotIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
