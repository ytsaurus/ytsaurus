#include "private.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger JobProxyClientLogger("JobProxyClient");

TString GetDefaultJobsMetaContainerName()
{
    return "yt_jobs_meta";
}

TString GetSlotMetaContainerName(int slotIndex)
{
    return Format("slot_meta_%v", slotIndex);
}

TString GetFullSlotMetaContainerName(const TString& jobsMetaName, int slotIndex)
{
    return Format(
        "%v/%v",
        jobsMetaName,
        GetSlotMetaContainerName(slotIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
