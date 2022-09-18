#pragma once

#include "public.h"

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeMutationRecord(
    const NProto::TMutationHeader& mutationHeader,
    TRef mutationData);

void DeserializeMutationRecord(
    const TSharedRef& recordData,
    NProto::TMutationHeader* mutationHeader,
    TSharedRef* mutationData);

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const NProto::TSnapshotMeta& meta,
    NYson::IYsonConsumer* consumer);
void Deserialize(
    NProto::TSnapshotMeta& meta,
    NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

