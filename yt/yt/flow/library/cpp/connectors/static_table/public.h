#pragma once

#include "source_public.h"

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow::NStaticTableConnector {

////////////////////////////////////////////////////////////////////////////////

class TArrivalOrderTableSink;
class TArrivalOrderTableSinkController;

DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicArrivalOrderTableSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkOwner);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkPartitionProgress);
DECLARE_REFCOUNTED_STRUCT(TArrivalOrderTableSinkProgress);

} // namespace NYT::NFlow::NStaticTableConnector
