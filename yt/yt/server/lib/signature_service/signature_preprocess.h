#pragma once

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] std::vector<std::byte> PreprocessSignature(
    const NYson::TYsonString& header,
    const NYson::TYsonString& payload);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService
