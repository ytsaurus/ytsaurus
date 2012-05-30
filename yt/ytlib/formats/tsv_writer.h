#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/misc/enum.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TTsvWriter
    : public NYTree::TYsonConsumerBase
{
public:
    explicit TTsvWriter(TOutputStream* stream, TTsvFormatConfigPtr config = New<TTsvFormatConfig>());

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
    TTsvFormatConfigPtr Config;

    bool FirstLine;
    bool FirstItem;

    DECLARE_ENUM(EState,
        (ExpectListItem)
        (ExpectBeginMap)
        (ExpectKey)
        (AfterKey)
    );

    EState State;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
