#include "stdafx.h"
#include "null_yson_consumer.h"
#include "yson_consumer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNullYsonConsumer
    : public IYsonConsumer
{
    virtual void OnStringScalar(const TStringBuf& value)
    {
        UNUSED(value);

    }

    virtual void OnIntegerScalar(i64 value)
    {
        UNUSED(value);

    }

    virtual void OnDoubleScalar(double value)
    {
        UNUSED(value);

    }
    
    virtual void OnEntity()
    {

    }

    virtual void OnBeginList()
    { }

    virtual void OnListItem()
    { }
    
    virtual void OnEndList()
    {

    }

    virtual void OnBeginMap()
    { }
    
    virtual void OnMapItem(const TStringBuf& name)
    {
        UNUSED(name);
    }

    virtual void OnEndMap()
    {

    }

    virtual void OnBeginAttributes()
    { }

    virtual void OnAttributesItem(const TStringBuf& name)
    {
        UNUSED(name);
    }

    virtual void OnEndAttributes()
    { }
};

IYsonConsumer* GetNullYsonConsumer()
{
    return Singleton<TNullYsonConsumer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
