#include "profile.h"

#include <util/stream/zlib.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

void WriteProfile(IOutputStream* out, const NProto::Profile& profile)
{
    TZLibCompress compress(out, ZLib::StreamType::GZip);
    profile.SerializeToArcadiaStream(&compress);
    compress.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
