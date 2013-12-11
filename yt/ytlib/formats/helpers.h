#pragma once

#include "public.h"

#include <ytlib/yson/consumer.h>
#include <ytlib/yson/parser.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TFormatsConsumerBase
    : public virtual NYson::TYsonConsumerBase
{
public:
    TFormatsConsumerBase();

    virtual void OnStringScalar(const TStringBuf& value) = 0;
    virtual void OnIntegerScalar(i64 value) = 0;
    virtual void OnDoubleScalar(double value) = 0;
    virtual void OnEntity() = 0;
    virtual void OnBeginList() = 0;
    virtual void OnListItem() = 0;
    virtual void OnEndList() = 0;
    virtual void OnBeginMap() = 0;
    virtual void OnKeyedItem(const TStringBuf& key) = 0;
    virtual void OnEndMap() = 0;
    virtual void OnBeginAttributes() = 0;
    virtual void OnEndAttributes() = 0;

    // This method has standard implementation for yamr, dsv and yamred dsv formats.
    virtual void OnRaw(const TStringBuf& yson, NYson::EYsonType type) override;

private:
    NYson::TStatelessYsonParser Parser;
};

////////////////////////////////////////////////////////////////////////////////

bool IsAscii(const TStringBuf& str);

Stroka Utf8ToByteString(const Stroka& str);

Stroka ByteStringToUtf8(const TStringBuf& str);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
