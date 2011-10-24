#include "stdafx.h"
#include "message.h"
#include "rpc.pb.h"

#include "../misc/serialize.h"
#include "../logging/log.h"

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

////////////////////////////////////////////////////////////////////////////////

TBlobMessage::TBlobMessage(TBlob* blob)
{
    Parts.push_back(TSharedRef(*blob));
}

TBlobMessage::TBlobMessage(TBlob* blob, const yvector<TRef>& parts)
{
    TSharedRef::TBlobPtr sharedBlob = new TBlob();
    blob->swap(*sharedBlob);
    FOREACH(const auto& it, parts) {
        Parts.push_back(TSharedRef(sharedBlob, it));
    }
}

const yvector<TSharedRef>& TBlobMessage::GetParts()
{
    return Parts;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
