#include "profile.h"

#include <util/stream/zlib.h>

namespace NYT::NProf {

////////////////////////////////////////////////////////////////////////////////

void WriteProfile(IOutputStream* out, const Profile& profile)
{
    TZLibCompress compress(out, ZLib::StreamType::GZip);
    profile.SerializeToArcadiaStream(&compress);
    compress.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProf
