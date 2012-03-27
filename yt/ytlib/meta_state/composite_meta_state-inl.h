#ifndef COMPOSITE_META_STATE_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_meta_state.h"
#endif

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TMessage, class TResult>
void TMetaStatePart::RegisterMethod(
    TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod)
{
    YASSERT(changeMethod);

    Stroka changeType = TMessage().GetTypeName();
    auto action = FromMethod(
        &TMetaStatePart::MethodThunk<TMessage, TResult>,
        this,
        changeMethod);
    YVERIFY(MetaState->Methods.insert(MakePair(changeType, action)).second == 1);
}

template <class TMessage, class TResult>
void TMetaStatePart::MethodThunk(
    const TRef& changeData,
    typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod)
{
    YASSERT(changeMethod);

    TMessage message;
    YVERIFY(DeserializeFromProtobuf(&message, changeData));

    changeMethod->Do(message);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
