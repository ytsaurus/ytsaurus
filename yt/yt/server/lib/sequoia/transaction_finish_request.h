#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTransactionFinishRequest;

} // namespace NProto

// NB: this enum is used in yt/yt/server/lib/sequoia/proto/transaction_manager.proto.
DEFINE_ENUM(ETransactionFinishRequest,
    ((Commit)       (0))
    ((Abort)        (1))
    ((Expiration)   (2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
