#ifndef COMPOSITE_META_STATE_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_meta_state.h"
#endif

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TMessage, class TResult>
void TMetaStatePart::RegisterMethod(
    TCallback<TResult(const NProto::TChangeHeader& header, const TMessage&)> changeMethod)
{
    Stroka changeType = TMessage().GetTypeName();
    auto action = BIND(
        &TMetaStatePart::MethodThunkWithHeader<TMessage, TResult>,
        Unretained(this),
        MoveRV(changeMethod));
    YCHECK(MetaState->Methods.insert(MakePair(changeType, action)).second == 1);
}

template <class TMessage, class TResult>
void TMetaStatePart::RegisterMethod(
    TCallback<TResult(const TMessage&)> changeMethod)
{
    Stroka changeType = TMessage().GetTypeName();
    auto action = BIND(
        &TMetaStatePart::MethodThunkWithoutHeader<TMessage, TResult>,
        Unretained(this),
        MoveRV(changeMethod));
    YCHECK(MetaState->Methods.insert(MakePair(changeType, action)).second == 1);
}

template <class TMessage, class TResult>
void TMetaStatePart::MethodThunkWithHeader(
    TCallback<TResult(const NProto::TChangeHeader& header, const TMessage& message)> changeMethod,
    const NProto::TChangeHeader& header,
    const TRef& changeData)
{
    TMessage message;
    YCHECK(DeserializeFromProto(&message, changeData));

    changeMethod.Run(header, message);
}

template <class TMessage, class TResult>
void TMetaStatePart::MethodThunkWithoutHeader(
    TCallback<TResult(const TMessage& message)> changeMethod,
    const NProto::TChangeHeader& header,
    const TRef& changeData)
{
    UNUSED(header);

    TMessage message;
    YCHECK(DeserializeFromProto(&message, changeData));

    changeMethod.Run(message);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
