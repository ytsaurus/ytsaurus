#pragma once

#include "public.h"

#include <core/misc/public.h>
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

    ~TResponseKeeper();

    TSharedRefArray FindResponse(const TMutationId& id);

    void RegisterResponse(
        const TMutationId& id,
        const TSharedRefArray& data,
        TInstant now = Now());

    void RemoveExpiredResponses(TInstant now = Now());

    void Clear();

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
