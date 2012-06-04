#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/misc/enum.h>

#include <library/json/json_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TJsonWriter
    : public NYTree::TYsonConsumerBase
{
public:
    explicit TJsonWriter(TOutputStream* stream, TJsonFormatConfigPtr config = NULL);
    ~TJsonWriter();

    // IYsonConsumer overrides.
    virtual void OnStringScalar(const TStringBuf& value);
    virtual void OnIntegerScalar(i64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnEntity();
    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList();
    virtual void OnBeginMap();
    virtual void OnKeyedItem(const TStringBuf& key);
    virtual void OnEndMap();
    virtual void OnBeginAttributes();
    virtual void OnEndAttributes();

private:
    TOutputStream* Stream;
    TJsonFormatConfigPtr Config;

    THolder<NJson::TJsonWriter> Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
