#pragma once

#include "public.h"

#include <yt/ytlib/hydra/hydra_manager.pb.h>

#include <yt/core/misc/ref.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeMutationRecord(
    const NProto::TMutationHeader& mutationHeader,
    const TRef& mutationData);

void DeserializeMutationRecord(
    const TSharedRef& recordData,
    NProto::TMutationHeader* mutationHeader,
    TSharedRef* mutationData);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

