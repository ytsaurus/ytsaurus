#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IVersionProvider)

struct IVersionProvider
    : public TRefCounted
{
    //! Returns a fresh, strictly increasing version.
    //! May context-switch and must be called from a fiber.
    virtual TVersion GenerateVersion() = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionProvider)

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
class TVersionedValue
    : public NYTree::TYsonStruct
{
public:
    TVersion GetVersion() const;

    const TValue& GetValue() const;

    TInstant GetLastUpdate() const;

    //! Replaces the value and advances its version if the content differs. Returns whether it changed.
    bool TrySetValue(TValue newValue, const IVersionProviderPtr& versionProvider);

    //! Advances the version after an in-place mutation.
    void Bump(const IVersionProviderPtr& versionProvider);

    REGISTER_YSON_STRUCT(TVersionedValue<TValue>);

    static void Register(TRegistrar registrar);

protected:
    TVersion Version_ = TVersion(0);
    TValue Value_;
    TInstant LastUpdate_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define VERSIONED_VALUE_H_
#include "versioned_value-inl.h"
#undef VERSIONED_VALUE_H_
