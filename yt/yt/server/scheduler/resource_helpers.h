#pragma once

#include "public.h"

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/node.h>

#include <yt/yt/core/misc/string_builder.h>

#include <yt/yt/library/vector_hdrf/resource_vector.h>
#include <yt/yt/library/vector_hdrf/resource_volume.h>

namespace NYT {

namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const TJobResourcesConfigPtr& config, TJobResources defaultValue);

////////////////////////////////////////////////////////////////////////////////

// For each memory capacity gives the number of nodes with this much memory.
using TMemoryDistribution = THashMap<i64, int>;

TJobResources GetAdjustedResourceLimits(
    const TJobResources& demand,
    const TJobResources& limits,
    const TMemoryDistribution& execNodeMemoryDistribution);

////////////////////////////////////////////////////////////////////////////////

void ProfileResourceVector(
    NProfiling::ISensorWriter* writer,
    const THashSet<EJobResourceType>& resourceTypes,
    const TResourceVector& resourceVector,
    const TString& prefix);

////////////////////////////////////////////////////////////////////////////////

void ProfileResourceVolume(
    NProfiling::ISensorWriter* writer,
    const TResourceVolume& volume,
    const TString& prefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler

namespace NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TResourceVolume& volume, NYson::IYsonConsumer* consumer);
void Deserialize(TResourceVolume& volume, NYTree::INodePtr node);

void Serialize(const TResourceVector& resourceVector, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const TResourceVolume& volume, TStringBuf /* format */);
TString ToString(const TResourceVolume& volume);

void FormatValue(TStringBuilderBase* builder, const TResourceVector& resourceVector, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVectorHdrf

} // namespace NYT


