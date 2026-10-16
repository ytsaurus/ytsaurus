#pragma once

#include "private.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

void BuildQueueStatusYson(
    const TQueueSnapshotPtr& snapshot,
    const NAlertManager::IAlertManagerPtr& alertManager,
    const TErrorOr<THashMap<std::string, TQueueExportProgressPtr>>& queueExportsProgressOrError,
    NYTree::TFluentAny fluent);
void BuildQueuePartitionListYson(const TQueueSnapshotPtr& snapshot, NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

void BuildConsumerStatusYson(const TConsumerSnapshotPtr& snapshot, NYTree::TFluentAny fluent);
void BuildConsumerPartitionListYson(const TConsumerSnapshotPtr& snapshot, NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

void BuildChildOrErrorYson(
    TStringBuf key,
    NYTree::TFluentMap fluent,
    const std::pair<const std::string, TErrorOr<NYTree::IMapNodePtr>>& pair);

void BuildMultiConsumerStatusYson(
    const TMultiConsumerSnapshotPtr& snapshot,
    const THashMap<std::string, TErrorOr<NYTree::IMapNodePtr>>& consumerOrchids,
    const NAlertManager::IAlertManagerPtr& alertManager,
    NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
