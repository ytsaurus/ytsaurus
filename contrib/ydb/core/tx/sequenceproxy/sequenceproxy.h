#pragma once
#include "defs.h"

#include <contrib/ydb/core/tx/sequenceproxy/public/events.h>

namespace NKikimr {
namespace NSequenceProxy {

    struct TSequenceProxySettings {
        // TODO: add settings for sequence proxy
    };

    IActor* CreateSequenceProxy(const TSequenceProxySettings& settings = {});

} // namespace NSequenceProxy
} // namespace NKikimr
