#pragma once

#include "public.h"

#include <yt/yt/ytlib/hydra2/proto/hydra_manager.pb.h>

#include <yt/yt/core/misc/ref.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeMutationRecord(
    const NProto::TMutationHeader& mutationHeader,
    TRef mutationData);

void DeserializeMutationRecord(
    const TSharedRef& recordData,
    NProto::TMutationHeader* mutationHeader,
    TSharedRef* mutationData);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2

