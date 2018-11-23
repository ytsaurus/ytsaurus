#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ISecretVaultService
    : public virtual TRefCounted
{
    struct TSecretSubrequest
    {
        TString SecretId;
        TString SecretVersion;
        TString DelegationToken;
        TString Signature;
    };

    struct TSecretSubresponse
    {
        THashMap<TString, TString> Payload;
    };

    using TErrorOrSecretSubresponse = TErrorOr<TSecretSubresponse>;

    virtual TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(
        const std::vector<TSecretSubrequest>& subrequests) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISecretVaultService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
