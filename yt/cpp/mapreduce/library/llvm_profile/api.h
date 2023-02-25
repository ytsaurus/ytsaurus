#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/generic/string.h>

namespace NYtLlvmProfile {
    struct TOptions {
        TString Proxy;
        TString Token;
        TString TransactionID;
        ::NYT::TYPath Table;
        double JobSamplingRate{0.0};
    };

    /* Start collecting profile. May be called multiple times, each call will result in a new
     * profile.
     *
     * NOTE: Call this function inside operation ctor or `Start` method.
     */
    void StartProfiling(const TOptions& opts);

    void CancelProfiling() noexcept;

    /* Stop collecting profile and upload it to YT. Destination cluster and directory are specified
     * in a previous call to `StartProfiling`.
     *
     * NOTE: Call this function inside operation `Finish` method.
     */
    void StopAndDumpProfile() noexcept;
}
