#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/lexer.h>
#include <ytlib/misc/enum.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

// Note: line_prefix is only supported for tabular data

class TDsvWriter
    : public virtual NYTree::IYsonConsumer
{
public:
    explicit TDsvWriter(
        TOutputStream* stream,
        NYTree::EYsonType type = NYTree::EYsonType::ListFragment,
        TDsvFormatConfigPtr config = NULL);
    ~TDsvWriter();

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

    virtual void OnRaw(const TStringBuf& yson, NYTree::EYsonType type);

private:
    NYTree::EYsonType Type;

    TOutputStream* Stream;
    TDsvFormatConfigPtr Config;

    bool FirstLine;
    bool FirstItem;

    void EscapeAndWrite(const TStringBuf& key);
    const char* FindNextEscapedSymbol(const char* begin, const char* end);

    bool AllowBeginList;
    bool AllowBeginMap;

    bool IsStopSymbol[256];

    NYTree::TLexer Lexer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
