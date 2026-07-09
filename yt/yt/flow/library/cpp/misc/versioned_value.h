#pragma once

#include "public.h"
#include "version_helpers.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
class TVersionedValue
    : public NYTree::TYsonStruct
{
public:
    TVersion GetVersion() const;

    const TValue& GetValue() const;

    TInstant GetLastUpdate() const;

    void SetValue(TValue newValue);

    void BumpVersion();

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
