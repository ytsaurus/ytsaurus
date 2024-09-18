#pragma once

#include "public.h"

#include "crypto.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/public.h>

#include <array>

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

class TSignature
{
public:
    [[nodiscard]] const NYson::TYsonString& Payload() const noexcept;

private:
    NYson::TYsonString Header_;
    NYson::TYsonString Payload_;
    std::array<std::byte, SignatureSize> Signature_;

    friend class TSignatureGenerator;
    friend class TSignatureValidator;

    friend void Serialize(const TSignature& signature, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TSignature& signature, NYTree::INodePtr node);
    friend void Deserialize(TSignature& signature, NYson::TYsonPullParserCursor* cursor);
};
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService
