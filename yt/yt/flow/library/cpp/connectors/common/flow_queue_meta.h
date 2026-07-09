#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TFlowQueueMeta
    : public virtual NYTree::TYsonStructLite
{
    std::optional<TSystemTimestamp> EventTimestamp;
    std::vector<i64> EventTimestampDeltas;
    std::optional<TSystemTimestamp> EventWatermark;
    bool PureHeartbeat{};

    REGISTER_YSON_STRUCT_LITE(TFlowQueueMeta);

    static void Register(TRegistrar registrar);
};

TFlowQueueMeta BuildFlowQueueMeta(const std::vector<TOutputMessageConstPtr>& messages);
TFlowQueueMeta BuildFlowQueueMeta(const TMessage& message);

void ApplyFlowQueueMeta(const TFlowQueueMeta& meta, TMessage& message, int index);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
