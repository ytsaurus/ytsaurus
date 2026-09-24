#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/connectors/static_table/source.h>

namespace NYT::NFlow::NStaticTableConnectorV2 {

class TSource
    : public NStaticTableConnector::TSource
{
public:
    using NStaticTableConnector::TSource::TSource;
};

} // namespace NYT::NFlow::NStaticTableConnectorV2
