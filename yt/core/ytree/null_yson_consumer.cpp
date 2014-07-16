#include "stdafx.h"
#include "null_yson_consumer.h"

#include <core/yson/consumer.h>
#include "yson_string.h"

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TNullYsonConsumer
    : public IYsonConsumer
{
    virtual void OnStringScalar(const TStringBuf& value) override
    {
        UNUSED(value);
    }

    virtual void OnInt64Scalar(i64 value) override
    {
        UNUSED(value);
    }

    virtual void OnDoubleScalar(double value) override
    {
        UNUSED(value);
    }

    virtual void OnBooleanScalar(bool value) override
    {
        UNUSED(value);
    }

    virtual void OnEntity() override
    { }

    virtual void OnBeginList() override
    { }

    virtual void OnListItem() override
    { }

    virtual void OnEndList() override
    { }

    virtual void OnBeginMap() override
    { }

    virtual void OnKeyedItem(const TStringBuf& name) override
    {
        UNUSED(name);
    }

    virtual void OnEndMap() override
    { }

    virtual void OnBeginAttributes() override
    { }

    virtual void OnEndAttributes() override
    { }

    virtual void OnRaw(const TStringBuf& yson, EYsonType type)
    {
        UNUSED(yson);
        UNUSED(type);
    }
};

IYsonConsumer* GetNullYsonConsumer()
{
    return Singleton<TNullYsonConsumer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
