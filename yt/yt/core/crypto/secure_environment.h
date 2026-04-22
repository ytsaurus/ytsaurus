#pragma once

#include <yt/yt/core/ytree/attributes.h>

#include <util/generic/strbuf.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

//! Returns the singleton thread-safe attribute dictionary for secure environment variables.
/*!
 *  Provides a secure in-process storage for sensitive configuration values
 *  (tokens, keys, certificates, etc.) that can be populated at server start
 *  or at any later point.
 *
 *  Implements IAttributeDictionary (like TEphemeralAttributeDictionary) but
 *  allocates YSON value storage from a TChunkedMemoryPool backed by undumpable
 *  memory, so that sensitive data is excluded from core dumps.
 *
 *  Memory used for storing values is zeroed at removing or overwriting value,
 *  and never freed or reused for new secrets, thus dangling pointers stay safe.
 *
 *  The returned dictionary is thread-safe.
 *
 *  Usage:
 *    auto env = GetSecureEnvironment();
 *    env->Set("YT_TOKEN", token);
 *    auto token = env->Find<TString>("YT_TOKEN");
 */
const NYTree::IAttributeDictionaryPtr& GetSecureEnvironment();

//! Move into secure environment variables with given names or prefixes.
//! Original environment variable values are filled with '*' characters.
/*!
 *  Usage:
 *  MoveToSecureEnvironment(
 *    std::vector<TStringBuf>{"YT_TOKEN"},
 *    std::vector<TStringBuf>{"YT_SECURE_VAULT"},
 *  );
 */
void MoveToSecureEnvironment(const std::vector<TStringBuf>& names, const std::vector<TStringBuf>& namePrefixes = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
