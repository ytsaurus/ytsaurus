#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TStreamSpeedStatistics
    : public NYTree::TYsonStructLite
{
    double ProcessedMessagesPerSecond{};
    double ProcessedBytesPerSecond{};

    REGISTER_YSON_STRUCT_LITE(TStreamSpeedStatistics);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
