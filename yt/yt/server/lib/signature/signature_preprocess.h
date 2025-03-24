#pragma once

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] std::vector<std::byte> PreprocessSignature(
    const NYson::TYsonString& header,
    const std::string& payload);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
