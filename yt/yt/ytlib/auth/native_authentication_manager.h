#pragma once

#include "public.h"

#include <yt/yt/library/auth_server/tvm_service.h>

#include <yt/yt/core/misc/atomic_object.h>

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
    TAtomicObject<IDynamicTvmServicePtr> TvmService_;
    std::atomic<bool> EnableValidation_ = false;
    std::atomic<bool> EnableSubmission_ = true;

    IDynamicTvmServicePtr CreateTvmService(const TTvmServiceConfigPtr& config);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
