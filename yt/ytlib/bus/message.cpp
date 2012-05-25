#include "stdafx.h"
#include "message.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

class TMessage
    : public IMessage
{
public:
    TMessage(const std::vector<TSharedRef>& parts)
        : Parts(parts)
    { }

    TMessage(std::vector<TSharedRef>&& parts)
        : Parts(ForwardRV< std::vector<TSharedRef> >(parts))
    { }

    virtual const std::vector<TSharedRef>& GetParts()
    {
        return Parts;
    }

private:
    std::vector<TSharedRef> Parts;

};

////////////////////////////////////////////////////////////////////////////////

IMessagePtr CreateMessageFromParts(const std::vector<TSharedRef>& parts)
{
    return New<TMessage>(parts);
}

IMessagePtr CreateMessageFromParts(std::vector<TSharedRef>&& parts)
{
    return New<TMessage>(parts);
}

IMessagePtr CreateMessageFromPart(const TSharedRef& part)
{
    std::vector<TSharedRef> parts;
    parts.push_back(part);
    return New<TMessage>(parts);
}

IMessagePtr CreateMessageFromParts(TBlob&& blob, const std::vector<TRef>& refs)
{
    TSharedRef sharedBlob(MoveRV(blob));
    std::vector<TSharedRef> parts(refs.size());
    for (int i = 0; i < static_cast<int>(refs.size()); ++i) {
        parts[i] = TSharedRef(sharedBlob, refs[i]);
    }
    return New<TMessage>(MoveRV(parts));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
