#pragma once

#include "public.h"
#include "forwarding_yson_consumer.h"
#include "yson_writer.h"
#include "yson_stream.h"

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
    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnIntegerScalar(i64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;

    virtual void OnMyKeyedItem(const TStringBuf& key) override;
    virtual void OnMyBeginMap() override;
    virtual void OnMyEndMap() override;
    virtual void OnMyBeginAttributes() override;
    virtual void OnMyEndAttributes()override;

private:
    IAttributeDictionary* Attributes;
    TStringStream Output;
    THolder<TYsonWriter> Writer;

    void ThrowMapExpected();

};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
