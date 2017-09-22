#include "null_consumer.h"
#include "string.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

void TNullYsonConsumer::OnStringScalar(const TStringBuf& /*value*/)
{ }

void TNullYsonConsumer::OnInt64Scalar(i64 /*value*/)
{ }

void TNullYsonConsumer::OnUint64Scalar(ui64 /*value*/)
{ }

void TNullYsonConsumer::OnDoubleScalar(double /*value*/)
{ }

void TNullYsonConsumer::OnBooleanScalar(bool /*value*/)
{ }

void TNullYsonConsumer::OnEntity()
{ }

void TNullYsonConsumer::OnBeginList()
{ }

void TNullYsonConsumer::OnListItem()
{ }

void TNullYsonConsumer::OnEndList()
{ }

void TNullYsonConsumer::OnBeginMap()
{ }

void TNullYsonConsumer::OnKeyedItem(const TStringBuf& /*name*/)
{ }

void TNullYsonConsumer::OnEndMap()
{ }

void TNullYsonConsumer::OnBeginAttributes()
{ }

void TNullYsonConsumer::OnEndAttributes()
{ }

void TNullYsonConsumer::OnRaw(const TStringBuf& /*yson*/, EYsonType /*type*/)
{ }

////////////////////////////////////////////////////////////////////////////////

IYsonConsumer* GetNullYsonConsumer()
{
    return Singleton<TNullYsonConsumer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
