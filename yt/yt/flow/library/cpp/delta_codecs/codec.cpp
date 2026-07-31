#include "codec.h"

#include "none.h"
#include "xdelta.h"

#include <library/cpp/yt/error/error.h>

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    switch (id) {
        case ECodec::None: {
            static TNoneCodec codec;
            return &codec;
        }
        case ECodec::XDelta: {
            static TXDeltaCodec codec;
            return &codec;
        }
        default:
            THROW_ERROR_EXCEPTION("Unsupported delta codec %Qlv",
                id);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDeltaCodecs
