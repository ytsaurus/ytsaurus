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
    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnIntegerScalar(i64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

    virtual void OnRaw(const TStringBuf& yson, NYTree::EYsonType type) override;

private:
    NYTree::EYsonType Type;

    TOutputStream* Stream;
    TDsvFormatConfigPtr Config;

    bool FirstLine;
    bool FirstItem;

    void EscapeAndWrite(const TStringBuf& key, const bool* IsStopSymbol);
    const char* FindNextEscapedSymbol(const char* begin, const char* end, const bool* IsStopSymbol);
    char EscapingTable[256];

    bool AllowBeginList;
    bool AllowBeginMap;

    bool IsKeyStopSymbol[256];
    bool IsValueStopSymbol[256];


    NYTree::TLexer Lexer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
