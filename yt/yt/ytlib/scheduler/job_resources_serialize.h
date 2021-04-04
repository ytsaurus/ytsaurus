#pragma once

#include "job_resources.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TExtendedJobResources& resources, NYson::IYsonConsumer* consumer);
void Serialize(const TJobResources& resources, NYson::IYsonConsumer* consumer);

void SerializeDiskQuota(
    const TDiskQuota& quota,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    NYson::IYsonConsumer* consumer);
void SerializeJobResourcesWithQuota(
    const TJobResourcesWithQuota& resources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    NYson::IYsonConsumer* consumer);

void Deserialize(TJobResources& resources, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
