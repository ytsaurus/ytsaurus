#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/connectors/static_table_v2/source.h>

namespace NYT::NFlow::NStaticTableConnector {

class TSource
    : public NStaticTableConnectorV2::TSource
{
public:
    using NStaticTableConnectorV2::TSource::TSource;
};

} // namespace NYT::NFlow::NStaticTableConnector
