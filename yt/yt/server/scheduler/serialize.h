#pragma once

#include "fair_share_update.h"

#include <yt/yt/library/vector_hdrf/resource_vector.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDetailedFairShare& detailedFairShare, NYson::IYsonConsumer* consumer);
void SerializeDominant(const TDetailedFairShare& detailedFairShare, NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
