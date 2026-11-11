#include "journal_nbd_helpers.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

TJournalNbdSpec ResolveJournalNbdSpec(
    const TJournalNbdOptionsPtr& /*journalNbdOptions*/,
    const NScheduler::TNbdDiskRequestPtr& /*diskRequest*/)
{
    THROW_ERROR_EXCEPTION("Journal NBD disks are not supported in this build");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
