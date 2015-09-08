#ifndef COMPOSITE_AUTOMATON_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_automaton.h"
#endif

#include "mutation.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

inline void TEntitySerializationKey::Save(TSaveContext& context) const
{
    NYT::Save(context, Index);
}

inline void TEntitySerializationKey::Load(TLoadContext& context)
{
    NYT::Load(context, Index);
}

////////////////////////////////////////////////////////////////////////////////

inline TEntitySerializationKey TSaveContext::GenerateSerializationKey()
{
    return TEntitySerializationKey(SerializationKeyIndex_++);
}

////////////////////////////////////////////////////////////////////////////////

inline TEntitySerializationKey TLoadContext::RegisterEntity(TEntityBase* entity)
{
    auto key = TEntitySerializationKey{static_cast<int>(Entities_.size())};
    Entities_.push_back(entity);
    return key;
}

template <class T>
T* TLoadContext::GetEntity(TEntitySerializationKey key) const
{
    YASSERT(key.Index >= 0 && key.Index < Entities_.size());
    return static_cast<T*>(Entities_[key.Index]);
}

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class TResponse>
void TCompositeAutomatonPart::RegisterMethod(
    TCallback<TResponse(const TRequest&)> callback)
{
    RegisterMethod(
        TRequest::default_instance().GetTypeName(),
        BIND(&TMutationActionTraits<TRequest, TResponse>::template Run<TCallback<TResponse(const TRequest&)>>, callback));
}

template <class TRequest, class TResponse>
void TCompositeAutomatonPart::RegisterMethod(
    TCallback<TResponse(TRequest&)> callback)
{
    RegisterMethod(
        TRequest::default_instance().GetTypeName(),
        BIND(&TMutationActionTraits<TRequest, TResponse>::template Run<TCallback<TResponse(TRequest&)>>, callback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
