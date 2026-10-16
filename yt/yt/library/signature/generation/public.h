#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TKeyRotatorConfig)
DECLARE_REFCOUNTED_STRUCT(TSignatureGeneratorConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressKeyWriterConfig)
DECLARE_REFCOUNTED_STRUCT(TSignatureGenerationConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TKeyRotator)
DECLARE_REFCOUNTED_CLASS(TSignatureGenerator)
DECLARE_REFCOUNTED_CLASS(TCypressKeyWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
