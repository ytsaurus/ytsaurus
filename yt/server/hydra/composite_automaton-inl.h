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

inline void TLoadContext::RegisterEntity(TEntityBase* entity)
{
    Entities_.push_back(entity);
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
    TCallback<TResponse(const TRequest&)> handler)
{
    auto wrappedHandler = BIND(
        &TMutationActionTraits<TRequest, TResponse>::Run,
        std::move(handler));
    YCHECK(Automaton->Methods.insert(std::make_pair(
        TRequest::default_instance().GetTypeName(),
        std::move(wrappedHandler))).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
