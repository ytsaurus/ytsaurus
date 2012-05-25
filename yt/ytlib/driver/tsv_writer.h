#pragma once

#include "public.h"

#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/misc/enum.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TTsvWriterConfig
    : public TConfigurable
{
    Stroka NewLineSeparator;
    Stroka KeyValueSeparator;
    Stroka ItemSeparator;

    TTsvWriterConfig()
    {
        Register("newline", NewLineSeparator).Default("\n");
        Register("key_value", KeyValueSeparator).Default("=");
        Register("item", ItemSeparator).Default("\t");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTsvWriter
    : public NYTree::IYsonConsumer
{
public:
    explicit TTsvWriter(TOutputStream* stream, TTsvWriterConfigPtr config = NULL);

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
    virtual void OnRaw(const TStringBuf& yson, NYTree::EYsonType type = NYTree::EYsonType::Node);

private:
    TOutputStream* Stream;
    TTsvWriterConfigPtr Config;

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
            
} // namespace NDriver
} // namespace NYT
