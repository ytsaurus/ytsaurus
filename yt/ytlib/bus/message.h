#pragma once

#include "common.h"
#include "rpc.pb.h"

#include "../misc/enum.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct IMessage
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IMessage> TPtr;

    virtual ~IMessage() {}
    virtual const yvector<TSharedRef>& GetParts() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobMessage
    : public IMessage
{
private:
    yvector<TSharedRef> Parts;

public:
    TBlobMessage(TBlob& blob);
    TBlobMessage(TBlob& blob, yvector<TRef>& parts);

    const yvector<TSharedRef>& GetParts();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
