#pragma once

#include "public.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

//! Tries each authenticator in order; resolves on first success,
//! or with a combined error if all fail.
template <class TAuthenticator, class TCredentials, class TResult>
class TCompositeAuthSession;

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> GetByYPath(const NYTree::INodePtr& node, const NYPath::TYPath& path);

std::string GetCryptoHash(TStringBuf secret);

std::string FormatUserIP(const NNet::TNetworkAddress& address);

std::string GetBlackboxCacheKeyFactorFromUserIP(
    EBlackboxCacheKeyMode mode,
    const NNet::TNetworkAddress& address);

std::string GetLoginForTvmId(TTvmId tvmId);

////////////////////////////////////////////////////////////////////////////////

class TSafeUrlBuilder
{
public:
    void AppendString(TStringBuf str);
    void AppendChar(char ch);
    void AppendParam(TStringBuf key, TStringBuf value);

    std::string FlushRealUrl();
    std::string FlushSafeUrl();

private:
    TStringBuilder RealUrl_;
    TStringBuilder SafeUrl_;
};

////////////////////////////////////////////////////////////////////////////////

struct THashedCredentials
{
    std::optional<std::string> TokenHash;
    // TODO(max42): add remaining fields from TCredentialsExt when needed.
};

THashedCredentials HashCredentials(const NRpc::NProto::TCredentialsExt& credentialsExt);

void Serialize(const THashedCredentials& hashedCredentials, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

std::string SignCsrfToken(
    const std::string& userId,
    const std::string& key,
    TInstant now);
TError CheckCsrfToken(
    const std::string& csrfToken,
    const std::string& userId,
    const std::string& key,
    TInstant expirationTime);

////////////////////////////////////////////////////////////////////////////////

//! Applies transformation described in the config to produce the output string.
std::string ApplyStringReplacement(const std::string& input, const TStringReplacementConfigPtr& replacement, const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

TError EnsureUserExists(
    bool createIfNotExists,
    const ICypressUserManagerPtr& userManager,
    const std::string& name,
    const std::vector<std::string>& tags);

////////////////////////////////////////////////////////////////////////////////

bool TryAddUserInGroups(
    const ICypressUserManagerPtr& userManager,
    const std::string& name,
    const std::vector<std::string>& groups);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
