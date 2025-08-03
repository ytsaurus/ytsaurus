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
        std::string SecretId;
        std::string SecretVersion;
        std::string DelegationToken;
        std::string Signature;
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
        std::string Key;
        std::string Value;
        std::string Encoding;
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
        std::string UserTicket;
        std::string SecretId;
        std::string Signature;
        std::string Comment;
        std::optional<TTvmId> TvmId;
    };

    struct TDelegationTokenResponse
    {
        std::string Token;
        TTvmId TvmId;
    };

    virtual TFuture<TDelegationTokenResponse> GetDelegationToken(
        TDelegationTokenRequest request) = 0;

    struct TRevokeDelegationTokenRequest
    {
        std::string DelegationToken;
        std::string SecretId;
        std::string Signature;
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
