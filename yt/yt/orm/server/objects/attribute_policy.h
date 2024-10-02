#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/key.h>

#include <util/system/defaults.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
struct IAttributePolicy
    : public NYT::TRefCounted
{
    virtual EAttributeGenerationPolicy GetGenerationPolicy() const = 0;

    virtual void Validate(
        const TValue& keyField,
        std::string_view title) const = 0;

    virtual TValue Generate(
        TTransaction* transaction,
        std::string_view title) const = 0;
};

struct TAttributePolicyOptions
{
    TObjectTypeValue Type = TObjectTypeValues::Null;
    TString AttributePath;
    TString IndexForIncrement;
};

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<IAttributePolicy<TString>> CreateStringAttributePolicy(
    EAttributeGenerationPolicy policy,
    size_t minLength,
    size_t maxLength,
    std::string_view charset);

// Integers in range [minValue; maxValue].
template <typename TInteger>
TIntrusivePtr<IAttributePolicy<TInteger>> CreateIntegerAttributePolicy(
    EAttributeGenerationPolicy policy,
    NMaster::IBootstrap* bootstrap,
    TInteger minValue,
    TInteger maxValue,
    TAttributePolicyOptions options = {});

////////////////////////////////////////////////////////////////////////////////

// Generates a random 16-character alphanumeric id.
TString RandomStringId();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define ATTRIBUTE_POLICY_INL_H_
#include "attribute_policy-inl.h"
#undef ATTRIBUTE_POLICY_INL_H_
