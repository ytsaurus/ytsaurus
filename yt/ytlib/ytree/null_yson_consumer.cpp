#include "stdafx.h"
#include "null_yson_consumer.h"
#include "yson_consumer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNullYsonConsumer
    : public IYsonConsumer
{
    virtual void OnStringScalar(const TStringBuf& value, bool hasAttributes = false)
    {
        UNUSED(value);
        UNUSED(hasAttributes);
    }

    virtual void OnIntegerScalar(i64 value, bool hasAttributes = false)
    {
        UNUSED(value);
        UNUSED(hasAttributes);
    }

    virtual void OnDoubleScalar(double value, bool hasAttributes = false)
    {
        UNUSED(value);
        UNUSED(hasAttributes);
    }
    
    virtual void OnEntity(bool hasAttributes = false)
    {
        UNUSED(hasAttributes);
    }

    virtual void OnBeginList()
    { }

    virtual void OnListItem()
    { }
    
    virtual void OnEndList(bool hasAttributes = false)
    {
        UNUSED(hasAttributes);
    }

    virtual void OnBeginMap()
    { }
    
    virtual void OnMapItem(const TStringBuf& name)
    {
        UNUSED(name);
    }

    virtual void OnEndMap(bool hasAttributes = false)
    {
        UNUSED(hasAttributes);
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
