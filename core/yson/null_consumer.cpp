#include "null_consumer.h"
#include "consumer.h"
#include "string.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TNullYsonConsumer
    : public IYsonConsumer
{
    virtual void OnStringScalar(const TStringBuf& /*value*/) override
    { }

    virtual void OnInt64Scalar(i64 /*value*/) override
    { }

    virtual void OnUint64Scalar(ui64 /*value*/) override
    { }

    virtual void OnDoubleScalar(double /*value*/) override
    { }

    virtual void OnBooleanScalar(bool /*value*/) override
    { }

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

    virtual void OnKeyedItem(const TStringBuf& /*name*/) override
    { }

    virtual void OnEndMap() override
    { }

    virtual void OnBeginAttributes() override
    { }

    virtual void OnEndAttributes() override
    { }

    virtual void OnRaw(const TStringBuf& /*yson*/, EYsonType /*type*/)
    { }
};

IYsonConsumer* GetNullYsonConsumer()
{
    return Singleton<TNullYsonConsumer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
