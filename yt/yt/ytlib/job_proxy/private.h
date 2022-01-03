#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAnyToYsonConverter)

inline const NLogging::TLogger JobProxyClientLogger("JobProxyClient");

TString GetDefaultJobsMetaContainerName();

TString GetSlotMetaContainerName(int slotIndex);
TString GetFullSlotMetaContainerName(const TString& jobsMetaName, int slotIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
