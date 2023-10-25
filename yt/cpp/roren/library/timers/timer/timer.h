#pragma once

#include <util/generic/string.h>
#include <yt/cpp/roren/library/timers/timer/proto/timers.pb.h>

namespace NRoren::NPrivate
{

////////////////////////////////////////////////////////////////////////////////

struct TTimer:
    public TTimerProto
{
    using TShardId = uint64_t;
    using TRawKey = TString;
    using TTimerId = TString;
    using TCallbackId = TString;
    using TTimestamp = uint64_t;
    using TUserData = std::optional<TString>;

    enum class EMergePolicy
    {
        REPLACE,
        MIN,
        MAX
    };

    using TKey = TTimerKeyProto;
    using TValue = TTimerValueProto;

    TTimer(TTimerProto timerProto);
    TTimer(TTimer&&) = default;
    TTimer(const TTimer&) = default;
    TTimer(const TRawKey& rawKey, const TTimerId& timerId, const TCallbackId& callbackId, const TTimestamp& timestamp, const TUserData& userData);
    TTimer& operator = (TTimer&&) = default;

    bool operator == (const TTimer& other) const noexcept;
    bool operator < (const TTimer& other) const noexcept;

    struct TKeyHasher
    {
        size_t operator () (const TKey& key) const;
    };

    struct TValueHasher
    {
        size_t operator () (const TValue& value) const;
    };

    struct THasher
    {
        size_t operator () (const TTimer& timer) const;
    };

    struct TKeyEqual
    {
        bool operator () (const TTimer::TKey& a, const TTimer::TKey& b);
    };
};  // TTimerData

struct TTimerContext
{
    TTimer Timer_;
};

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate

namespace NRoren
{

////////////////////////////////////////////////////////////////////////////////

using TTimer = NPrivate::TTimer;
using TTimerContext = NPrivate::TTimerContext;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

