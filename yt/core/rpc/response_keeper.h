#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeper
    : public TRefCounted
{
public:
    explicit TResponseKeeper(
        TResponseKeeperConfigPtr config,
        const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

    TSharedRefArray FindResponse(const TMutationId& id);

    void RegisterResponse(
        const TMutationId& id,
        const TSharedRefArray& data,
        TInstant now = Now());

    void RemoveExpiredResponses(TInstant now = Now());

    void Clear();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
