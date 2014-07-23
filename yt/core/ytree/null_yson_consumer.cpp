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
    virtual void OnStringScalar(const TStringBuf& value)
    {
        UNUSED(value);
    }

    virtual void OnInt64Scalar(i64 value)
    {
        UNUSED(value);
    }

    virtual void OnDoubleScalar(double value)
    {
        UNUSED(value);
    }

    virtual void OnEntity()
    { }

    virtual void OnBeginList()
    { }

    virtual void OnListItem()
    { }

    virtual void OnEndList()
    { }

    virtual void OnBeginMap()
    { }

    virtual void OnKeyedItem(const TStringBuf& name)
    {
        UNUSED(name);
    }

    virtual void OnEndMap()
    { }

    virtual void OnBeginAttributes()
    { }

    virtual void OnEndAttributes()
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
