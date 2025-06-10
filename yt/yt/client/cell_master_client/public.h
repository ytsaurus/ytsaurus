#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NCellMasterClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TCellDirectory;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

// Keep these two enums consistent.

DEFINE_BIT_ENUM(EMasterCellRoles,
    ((None)                      (0x0000))
    ((CypressNodeHost)           (0x0001))
    ((TransactionCoordinator)    (0x0002))
    ((ChunkHost)                 (0x0004))
    ((DedicatedChunkHost)        (0x0008))
    ((ExTransactionCoordinator)  (0x0010))
    ((SequoiaNodeHost)           (0x0020))
);

DEFINE_ENUM(EMasterCellRole,
    ((Unknown)                   (0x0000))

    ((CypressNodeHost)           (0x0001))
    ((TransactionCoordinator)    (0x0002))
    ((ChunkHost)                 (0x0004))
    ((DedicatedChunkHost)        (0x0008))
    ((ExTransactionCoordinator)  (0x0010))
    ((SequoiaNodeHost)           (0x0020))
);

DEFINE_ENUM_UNKNOWN_VALUE(EMasterCellRole, Unknown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
