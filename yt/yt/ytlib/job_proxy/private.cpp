#include "private.h"

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger JobProxyClientLogger("JobProxyClient");

TString GetDefaultJobsMetaContainerName()
{
    return "jm";
}

TString GetSlotMetaContainerName(int slotIndex)
{
    return Format("s_%v", slotIndex);
}

TString GetFullSlotMetaContainerName(const TString& jobsMetaName, int slotIndex)
{
    return Format(
        "%v/%v",
        jobsMetaName,
        GetSlotMetaContainerName(slotIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
