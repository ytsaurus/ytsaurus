#pragma once

#include "private.h"

#include <library/cpp/yt/misc/hash.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

template <class TMutation>
struct TBundleMutation;

////////////////////////////////////////////////////////////////////////////////

struct TBundleNameMixin
{
    std::string BundleName;

    auto operator<=>(const TBundleNameMixin& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

template <class TMutation>
concept CMutationWithBundleName = requires (TMutation mutation)
{
    mutation.BundleName;
};

template <class TMutation>
struct TBundleMutation
    : public TBundleNameMixin
{
    TMutation Mutation;

    TBundleMutation(const TBundleNameMixin& bundleNameMixin, TMutation mutation);
    TBundleMutation(std::string bundleName, TMutation mutation);

    TBundleMutation() = default;
    TBundleMutation(const TBundleMutation&) = default;
    TBundleMutation(TBundleMutation&&) = default;
    TBundleMutation& operator=(const TBundleMutation&) = default;
    TBundleMutation& operator=(TBundleMutation&&) = default;

    constexpr TMutation& operator->()
        requires (!CMutationWithBundleName<TMutation>);

    constexpr const TMutation& operator->() const
        requires (!CMutationWithBundleName<TMutation>);

    constexpr bool operator==(const TBundleMutation& other) const;
    constexpr auto operator<=>(const TBundleMutation& other) const;

    constexpr operator TMutation() const;
};

template <class TMutation, class T>
constexpr bool operator==(const TBundleMutation<TMutation>& lhs, const T& rhs);

template <class TMutation, class T>
constexpr auto operator<=>(const TBundleMutation<TMutation>& lhs, const T& rhs);

template <class TMutation, class T>
constexpr bool operator==(const T& lhs, const TBundleMutation<TMutation>& rhs);

template <class TMutation, class T>
constexpr auto operator<=>(const T& lhs, const TBundleMutation<TMutation>& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer

////////////////////////////////////////////////////////////////////////////////

template <class TMutation>
struct THash<NYT::NCellBalancer::TBundleMutation<TMutation>>
{
    std::size_t operator()(const NYT::NCellBalancer::TBundleMutation<TMutation>& object) const
    {
        using NYT::HashCombine;

        size_t result = 0;
        HashCombine(result, object.Mutation);
        HashCombine(result, object.BundleName);
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

#define BUNDLE_MUTATION_INL_H_
#include "bundle_mutation-inl.h"
#undef BUNDLE_MUTATION_INL_H_
