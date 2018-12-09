#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/hash.h>

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

        bool operator == (const TSecretSubrequest& other) const
        {
            return
                std::tie(SecretId, SecretVersion, DelegationToken, Signature) ==
                std::tie(other.SecretId, other.SecretVersion, other.DelegationToken, other.Signature);
        }

        operator size_t() const
        {
            size_t hash = 0;
            HashCombine(hash, SecretId);
            HashCombine(hash, SecretVersion);
            HashCombine(hash, DelegationToken);
            HashCombine(hash, Signature);
            return hash;
        }
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

