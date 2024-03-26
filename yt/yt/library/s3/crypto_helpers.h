#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NS3::NCrypto {

////////////////////////////////////////////////////////////////////////////////

// These are some auxiliary functions used in S3 authentication algorithms
// described in https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html.
// They are located in separated namespace to avoid collisions due to popular names.

//! Convert the string to lowercase.
TString Lowercase(const TString& string);

//! Lowercase base 16 encoding.
TString Hex(const TString& string);

//! SHA256Hash is secure Hash Algorithm (SHA) cryptographic hash function.
//! Sha256HashHex(s) is Hex(SHA256Hash(s)).
TString Sha256HashHex(TSharedRef data);
TString Sha256HashHex(const TString& string);

//! Computes HMAC by using the SHA256 algorithm with the signing key provided.
TString HmacSha256(TStringBuf key, TStringBuf message);

//! Remove any leading or trailing whitespace.
TString Trim(const TString& string);

//! URI encode every byte.
TString UriEncode(const TString& string, bool isObjectPath);

TString FormatTimeIso8601(TInstant time);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3::NCrypto
