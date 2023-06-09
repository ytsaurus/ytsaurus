#pragma once

#include "public.h"

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TNativeAuthenticationManager
{
public:
    TNativeAuthenticationManager() = default;

    static TNativeAuthenticationManager* Get();

    void Configure(const TNativeAuthenticationManagerConfigPtr& config);
    void Reconfigure(const TNativeAuthenticationManagerDynamicConfigPtr& config);

    IDynamicTvmServicePtr GetTvmService() const;
    bool IsValidationEnabled() const;
    bool IsSubmissionEnabled() const;

    void SetTvmService(IDynamicTvmServicePtr tvmService);

private:
    TAtomicIntrusivePtr<IDynamicTvmService> TvmService_;
    std::atomic<bool> EnableValidation_ = false;
    std::atomic<bool> EnableSubmission_ = true;

    IDynamicTvmServicePtr CreateTvmService(const TTvmServiceConfigPtr& config);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
