#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, JobProxyLogger, "JobProxy");

constexpr auto RpcServerShutdownTimeout = TDuration::Seconds(5);

class TOneShotFlag
{
public:
    void Set(bool value) noexcept;
    bool Get() const noexcept;

    void operator = (bool value) noexcept;

    operator bool () const noexcept;

private:
    std::optional<bool> Flag_;
};

// NB(pogorelov): No don't need in to be an atomic,
// cause it can be modified only once (and it modification will happen before it can be read).
inline TOneShotFlag DeliveyFencedWriteEnabled;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf JobNetworkInterface = "veth0";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TProxySignatureGenerator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy

