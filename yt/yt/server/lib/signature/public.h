#pragma once

#include <yt/yt/client/signature/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/guid.h>
#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TKeyId, TGuid)
YT_DEFINE_STRONG_TYPEDEF(TOwnerId, std::string)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TKeyRotatorConfig)
DECLARE_REFCOUNTED_STRUCT(TSignatureGeneratorConfig)
DECLARE_REFCOUNTED_STRUCT(TSignatureValidatorConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressKeyReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressKeyWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TSignatureGenerationConfig)
DECLARE_REFCOUNTED_STRUCT(TSignatureValidationConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TKeyInfo)
DECLARE_REFCOUNTED_CLASS(TKeyRotator)
DECLARE_REFCOUNTED_CLASS(TSignatureGenerator)
DECLARE_REFCOUNTED_CLASS(TSignatureValidator)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IKeyStoreReader)
DECLARE_REFCOUNTED_STRUCT(IKeyStoreWriter)

////////////////////////////////////////////////////////////////////////////////

// NB(pavook) this prefix should be pruned from all logs and core dumps.
constexpr std::string_view PrivateKeyPrefix = "!YT-PRIVATE!";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
