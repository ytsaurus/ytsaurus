#pragma once

#include "public.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> GetByYPath(const NYTree::INodePtr& node, const NYPath::TYPath& path);

TString GetCryptoHash(TStringBuf secret);

TString FormatUserIP(const NNet::TNetworkAddress& address);

TString GetBlackboxCacheKeyFactorFromUserIP(
    EBlackboxCacheKeyMode mode,
    const NNet::TNetworkAddress& address);

TString GetLoginForTvmId(TTvmId tvmId);

////////////////////////////////////////////////////////////////////////////////

class TSafeUrlBuilder
{
public:
    void AppendString(TStringBuf str);
    void AppendChar(char ch);
    void AppendParam(TStringBuf key, TStringBuf value);

    TString FlushRealUrl();
    TString FlushSafeUrl();

private:
    TStringBuilder RealUrl_;
    TStringBuilder SafeUrl_;
};

////////////////////////////////////////////////////////////////////////////////

struct THashedCredentials
{
    std::optional<TString> TokenHash;
    // TODO(max42): add remaining fields from TCredentialsExt when needed.
};

THashedCredentials HashCredentials(const NRpc::NProto::TCredentialsExt& credentialsExt);

void Serialize(const THashedCredentials& hashedCredentials, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

std::string SignCsrfToken(
    const std::string& userId,
    const TString& key,
    TInstant now);
TError CheckCsrfToken(
    const std::string& csrfToken,
    const std::string& userId,
    const TString& key,
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

} // namespace NYT::NAuth

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
