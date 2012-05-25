#pragma once

#include "public.h"
#include "forwarding_yson_consumer.h"
#include "yson_writer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TAttributeConsumer
    : public TForwardingYsonConsumer
{
public:
    explicit TAttributeConsumer(IAttributeDictionary* attributes);
    IAttributeDictionary* GetAttributes() const;

protected:
    virtual void OnMyKeyedItem(const TStringBuf& key);
    virtual void OnMyBeginMap();
    virtual void OnMyEndMap();
    virtual void OnMyBeginAttributes();
    virtual void OnMyEndAttributes();

private:
    IAttributeDictionary* Attributes;
    TStringOutput Output;
    TYsonWriter Writer;

    Stroka Key;
    TYson Value;
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
