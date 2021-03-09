#pragma once

#include "resource_vector.h"
#include "fair_share_update.h"

namespace NYT {

namespace NFairShare {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDetailedFairShare& detailedFairShare, NYson::IYsonConsumer* consumer);
void SerializeDominant(const TDetailedFairShare& detailedFairShare, NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFairShare

namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TResourceVector& resourceVector, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler

} // namespace NYT
