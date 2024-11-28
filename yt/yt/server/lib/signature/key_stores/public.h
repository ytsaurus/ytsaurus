#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NSignature {

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCypressKeyReaderConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressKeyWriterConfig)
DECLARE_REFCOUNTED_CLASS(TCypressKeyReader)
DECLARE_REFCOUNTED_CLASS(TCypressKeyWriter)

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TStubKeyStore)

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
