#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "dsv_symbols.h"

#include <ytlib/misc/enum.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

// Note: line_prefix is only supported for tabular data
// YsonNode is written as follows:
//  * Each element of list is ended with RecordSeparator
//  * Items in map are separated with FieldSeparator
//  * Key and Values in map are separated with KeyValueSeparator

class TDsvWriter
    : public virtual TFormatsConsumerBase
{
public:
    explicit TDsvWriter(
        TOutputStream* stream,
        NYson::EYsonType type = NYson::EYsonType::ListFragment,
        TDsvFormatConfigPtr config = New<TDsvFormatConfig>());
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

private:
    TOutputStream* Stream;
    NYson::EYsonType Type;
    TDsvFormatConfigPtr Config;

    TDsvSymbolTable SymbolTable;

    bool InsideFirstLine;
    bool InsideFirstItem;
    bool InsideAttributes;

    bool AllowBeginList;
    bool AllowBeginMap;

    NYson::TLexer Lexer;

    void EscapeAndWrite(const TStringBuf& key, const bool* IsStopSymbol);
    const char* FindNextEscapedSymbol(const char* begin, const char* end, const bool* IsStopSymbol);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
