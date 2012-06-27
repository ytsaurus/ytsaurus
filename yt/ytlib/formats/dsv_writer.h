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
// YsonNode is writtern as follows:
//  * Each element of list is ended with RecordSeparator
//  * Items in map are separated with FieldSeparator
//  * Key and Values in map are separated with KeyValueSeparator

class TDsvWriter
    : public NYTree::IYsonConsumer
{
public:
    explicit TDsvWriter(
        TOutputStream* stream,
        NYTree::EYsonType type = NYTree::EYsonType::ListFragment,
        TDsvFormatConfigPtr config = NULL);
    ~TDsvWriter();

    // IYsonConsumer overrides.
    virtual void OnStringScalar(const TStringBuf& value) OVERRIDE;
    virtual void OnIntegerScalar(i64 value) OVERRIDE;
    virtual void OnDoubleScalar(double value) OVERRIDE;
    virtual void OnEntity() OVERRIDE;
    virtual void OnBeginList() OVERRIDE;
    virtual void OnListItem() OVERRIDE;
    virtual void OnEndList() OVERRIDE;
    virtual void OnBeginMap() OVERRIDE;
    virtual void OnKeyedItem(const TStringBuf& key) OVERRIDE;
    virtual void OnEndMap() OVERRIDE;
    virtual void OnBeginAttributes() OVERRIDE;
    virtual void OnEndAttributes() OVERRIDE;

    virtual void OnRaw(const TStringBuf& yson, NYTree::EYsonType type) OVERRIDE;

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
