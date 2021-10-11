#pragma once

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/vector_hdrf/resource_vector.h>

#include <yt/yt/library/vector_hdrf/fair_share_update.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDetailedFairShare& detailedFairShare, NYson::IYsonConsumer* consumer);
void SerializeDominant(const TDetailedFairShare& detailedFairShare, NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
