#include "codec.h"

#include "codec_detail.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

int ICodec::GetTotalPartCount() const
{
    return GetDataPartCount() + GetParityPartCount();
}

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    if (auto* codec = FindCodec(id)) {
        return codec;
    }

    THROW_ERROR_EXCEPTION("Unsupported erasure codec %Qlv", id);
}

const std::vector<ECodec>& GetSupportedCodecIds()
{
    static const std::vector<ECodec> supportedCodecIds = [] {
        std::vector<ECodec> codecIds;
        for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
            if (FindCodec(codecId)) {
                codecIds.push_back(codecId);
            }
        }

        codecIds.push_back(ECodec::None);
        return codecIds;
    }();
    return supportedCodecIds;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

