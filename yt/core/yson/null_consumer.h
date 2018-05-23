#pragma once

#include "consumer.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

//! Consumer that does nothing.
class TNullYsonConsumer
    : public IYsonConsumer
{
    virtual void OnStringScalar(TStringBuf /*value*/) override;

    virtual void OnInt64Scalar(i64 /*value*/) override;

    virtual void OnUint64Scalar(ui64 /*value*/) override;

    virtual void OnDoubleScalar(double /*value*/) override;

    virtual void OnBooleanScalar(bool /*value*/) override;

    virtual void OnEntity() override;

    virtual void OnBeginList() override;

    virtual void OnListItem() override;

    virtual void OnEndList() override;

    virtual void OnBeginMap() override;

    virtual void OnKeyedItem(TStringBuf /*name*/) override;

    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;

    virtual void OnEndAttributes() override;

    virtual void OnRaw(TStringBuf /*yson*/, EYsonType /*type*/);
};

////////////////////////////////////////////////////////////////////////////////

//! Returns the singleton instance of the null consumer.
IYsonConsumer* GetNullYsonConsumer();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
