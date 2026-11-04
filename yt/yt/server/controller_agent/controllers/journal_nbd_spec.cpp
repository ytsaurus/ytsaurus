#include "journal_nbd_spec.h"

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

void TJournalNbdSpec::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Config);
    PHOENIX_REGISTER_FIELD(2, DeviceSize);
    PHOENIX_REGISTER_FIELD(3, BlockSize);
    PHOENIX_REGISTER_FIELD(4, Account);
    PHOENIX_REGISTER_FIELD(5, MemoryUsage);
}

PHOENIX_DEFINE_TYPE(TJournalNbdSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
