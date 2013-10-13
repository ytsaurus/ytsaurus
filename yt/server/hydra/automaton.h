#pragma once

#include "public.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct IAutomaton
    : public virtual TRefCounted
{
    virtual void SaveSnapshot(TOutputStream* output) = 0;
    virtual void LoadSnapshot(TInputStream* input) = 0;

    virtual void Clear() = 0;

    virtual void ApplyMutation(TMutationContext* context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
