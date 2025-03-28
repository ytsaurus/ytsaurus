#ifndef BUNDLE_MUTATION_INL_H_
#error "Direct inclusion of this file is not allowed, include bundle_mutation.h"
// For the sake of sane code completion.
#include "bundle_mutation.h"
#endif

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

template <class TMutation>
TBundleMutation<TMutation>::TBundleMutation(const TBundleNameMixin& bundleNameMixin, TMutation mutation)
    : TBundleNameMixin(bundleNameMixin)
    , Mutation(std::move(mutation))
{ }

template <class TMutation>
TBundleMutation<TMutation>::TBundleMutation(std::string bundleName, TMutation mutation)
    : TBundleNameMixin(std::move(bundleName))
    , Mutation(std::move(mutation))
{ }

template <class TMutation>
constexpr bool TBundleMutation<TMutation>::operator==(const TBundleMutation& other) const
{
    return Mutation == other.Mutation;
}

template <class TMutation>
constexpr auto TBundleMutation<TMutation>::operator<=>(const TBundleMutation& other) const
{
    return Mutation <=> other.Mutation;
}

template <class TMutation>
constexpr TBundleMutation<TMutation>::operator TMutation() const
{
    return Mutation;
}

template <class TMutation>
constexpr TMutation& TBundleMutation<TMutation>::operator->()
    requires (!CMutationWithBundleName<TMutation>)
{
    return Mutation;
}

template <class TMutation>
constexpr const TMutation& TBundleMutation<TMutation>::operator->() const
    requires (!CMutationWithBundleName<TMutation>)
{
    return Mutation;
}

template <class TMutation, class T>
constexpr bool operator==(const TBundleMutation<TMutation>& lhs, const T& rhs)
{
    return lhs.Mutation == rhs;
}

template <class TMutation, class T>
constexpr bool operator==(const T& lhs, const TBundleMutation<TMutation>& rhs)
{
    return rhs == lhs;
}

template <class TMutation, class T>
constexpr auto operator<=>(const TBundleMutation<TMutation>& lhs, const T& rhs)
{
    return lhs.Mutation <=> rhs;
}

template <class TMutation, class T>
constexpr auto operator<=>(const T& lhs, const TBundleMutation<TMutation>& rhs)
{
    return lhs <=> rhs.Mutation;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
