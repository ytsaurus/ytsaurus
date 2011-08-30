#pragma once

#include "common.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct IMessage
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IMessage> TPtr;

    virtual const yvector<TSharedRef>& GetParts() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobMessage
    : public IMessage
{
public:
    TBlobMessage(TBlob* blob);
    TBlobMessage(TBlob* blob, const yvector<TRef>& parts);

    const yvector<TSharedRef>& GetParts();

private: 
    yvector<TSharedRef> Parts;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
