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
    template <COneOfTypes<TKeys>>
    using TTupleElement = TValue;

    using TTuple = typename TKeys::template Map<TTupleElement>::template Wrap<std::tuple>;

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

    const TTuple& GetValues() const
    {
        return Values_;
    }

    TTuple& GetValues()
    {
        return Values_;
    }

    void SetValues(TTuple values)
    {
        Values_ = std::move(values);
    }

private:
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
    using TTuple = typename TKeys::template Map<TValue>::template Wrap<std::tuple>;

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

    const TTuple& GetValues() const
    {
        return Values_;
    }

    TTuple& GetValues()
    {
        return Values_;
    }

    void SetValues(TTuple values)
    {
        Values_ = std::move(values);
    }

private:
    TTuple Values_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NMpl
