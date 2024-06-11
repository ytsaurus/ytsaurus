#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/misc/hash.h>

namespace NYT::NAuth {

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
        std::optional<TTvmId> TvmId;

        auto operator <=> (const TSecretSubrequest& other) const = default;

        operator size_t() const
        {
            size_t hash = 0;
            HashCombine(hash, SecretId);
            HashCombine(hash, SecretVersion);
            HashCombine(hash, DelegationToken);
            HashCombine(hash, Signature);
            HashCombine(hash, TvmId);
            return hash;
        }
    };

    struct TSecretValue
    {
        TString Key;
        TString Value;
        TString Encoding;
    };

    struct TSecretSubresponse
    {
        std::vector<TSecretValue> Values;
    };

    using TErrorOrSecretSubresponse = TErrorOr<TSecretSubresponse>;

    virtual TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(
        const std::vector<TSecretSubrequest>& subrequests) = 0;

    struct TDelegationTokenRequest
    {
        TString UserTicket;
        TString SecretId;
        TString Signature;
        TString Comment;
        std::optional<TTvmId> TvmId;
    };

    struct TDelegationTokenResponse
    {
        TString Token;
        TTvmId TvmId;
    };

    virtual TFuture<TDelegationTokenResponse> GetDelegationToken(
        TDelegationTokenRequest request) = 0;

    struct TRevokeDelegationTokenRequest
    {
        TString DelegationToken;
        TString SecretId;
        TString Signature;
        std::optional<TTvmId> TvmId;
    };

    virtual void RevokeDelegationToken(TRevokeDelegationTokenRequest request) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISecretVaultService)

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilder* builder,
    const ISecretVaultService::TSecretSubrequest& subrequest,
    TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
