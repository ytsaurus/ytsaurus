#include "stdafx.h"
#include "message.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

////////////////////////////////////////////////////////////////////////////////

class TMessage
    : public IMessage
{
public:
    TMessage(const yvector<TSharedRef>& parts)
        : Parts(parts)
    { }

    TMessage(yvector<TSharedRef>&& parts)
        : Parts(ForwardRV< yvector<TSharedRef> >(parts))
    { }

    virtual const yvector<TSharedRef>& GetParts()
    {
        return Parts;
    }

private:
    yvector<TSharedRef> Parts;

};

////////////////////////////////////////////////////////////////////////////////

IMessage::TPtr CreateMessageFromParts(const yvector<TSharedRef>& parts)
{
    return New<TMessage>(parts);
}

IMessage::TPtr CreateMessageFromParts(yvector<TSharedRef>&& parts)
{
    return New<TMessage>(parts);
}

IMessage::TPtr CreateMessageFromPart(const TSharedRef& part)
{
    yvector<TSharedRef> parts;
    parts.push_back(part);
    return New<TMessage>(parts);
}

IMessage::TPtr CreateMessageFromParts(TBlob&& blob, const yvector<TRef>& refs)
{
    TSharedRef sharedBlob(MoveRV(blob));
    yvector<TSharedRef> parts(refs.ysize());
    for (int i = 0; i < refs.ysize(); ++i) {
        parts[i] = TSharedRef(sharedBlob, refs[i]);
    }
    return New<TMessage>(MoveRV(parts));
}

IMessage::TPtr CreateMessageFromSlice(IMessage* message, int sliceStart, int sliceSize)
{
    YASSERT(message);
    YASSERT(sliceStart >= 0 && sliceStart + sliceSize <= message->GetParts().ysize());

    auto parts = message->GetParts();
    yvector<TSharedRef> sliceParts(sliceSize);
    for (int i = 0; i < sliceSize; ++i) {
        sliceParts[i] = parts[i + sliceStart];
    }
    return New<TMessage>(MoveRV(sliceParts));
}

IMessage::TPtr CreateMessageFromSlice(IMessage* message, int sliceStart)
{
    YASSERT(message);
    YASSERT(sliceStart >= 0 && sliceStart <= message->GetParts().ysize());

    return CreateMessageFromSlice(
        message,
        sliceStart,
        message->GetParts().ysize() - sliceStart);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
