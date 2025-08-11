#pragma once

#include "public.h"

#include "types.h"

#include <tuple>

namespace NYT::NOrm::NMpl {

////////////////////////////////////////////////////////////////////////////////

template <CDistinctTypes TKeys, class TValue>
class TTypeToValueMap
{
public:
    TTypeToValueMap() = default;

    template <CInvocableForEachType<TKeys> TProducer>
    explicit TTypeToValueMap(const TProducer& producer)
        : Values_(TKeys::template Produce<std::tuple>(producer))
    { }

    template <COneOfTypes<TKeys> TKey>
    const TValue& Get() const
    {
        return std::get<TKeys::template IndexOf<TKey>>(Values_);
    }

    template <COneOfTypes<TKeys> TKey>
    TValue& Get()
    {
        return std::get<TKeys::template IndexOf<TKey>>(Values_);
    }

    template <COneOfTypes<TKeys> TKey>
    void Set(TValue value)
    {
        Get<TKey>() = std::move(value);
    }

private:
    template <COneOfTypes<TKeys>>
    using TTupleElement = TValue;

    using TTuple = typename TKeys::template Map<TTupleElement>::template Wrap<std::tuple>;

    TTuple Values_;
};

////////////////////////////////////////////////////////////////////////////////

// Ideally, template template parameter TValue would be constrained:
// template <COneOfTypes<TKeys>> class TValue. Unfortunately, clang 19+ makes
// constrained template template parameters really difficult to work with.
// See https://st.yandex-team.ru/YTORM-1431 for details.
template <CDistinctTypes TKeys, template <class> class TValue>
class TTypeToTemplateValueMap
{
public:
    TTypeToTemplateValueMap() = default;

    template <CInvocableForEachType<TKeys> TProducer>
    explicit TTypeToTemplateValueMap(const TProducer& producer)
        : Values_(TKeys::template Produce<std::tuple>(producer))
    { }

    template <COneOfTypes<TKeys> TKey>
    const TValue<TKey>& Get() const
    {
        return std::get<TKeys::template IndexOf<TKey>>(Values_);
    }

    template <COneOfTypes<TKeys> TKey>
    TValue<TKey>& Get()
    {
        return std::get<TKeys::template IndexOf<TKey>>(Values_);
    }

    template <COneOfTypes<TKeys> TKey>
    void Set(TValue<TKey> value)
    {
        Get<TKey>() = std::move(value);
    }

private:
    using TTuple = typename TKeys::template Map<TValue>::template Wrap<std::tuple>;

    TTuple Values_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NMpl
