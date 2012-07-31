#pragma once

#include <ytlib/misc/intrusive_ptr.h>

namespace NYT {

class ICodec;
typedef TIntrusivePtr<ICodec> TCodecPtr;

} // namespace NYT
