#pragma once

#include "private.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

void BuildQueueStatusYson(const TQueueSnapshotPtr& snapshot, NYTree::TFluentAny fluent);
void BuildQueuePartitionListYson(const TQueueSnapshotPtr& snapshot, NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

void BuildConsumerStatusYson(const TConsumerSnapshotPtr& snapshot, NYTree::TFluentAny fluent);
void BuildConsumerPartitionListYson(const TConsumerSnapshotPtr& snapshot, NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
