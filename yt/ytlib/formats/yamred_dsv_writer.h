#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "dsv_table.h"

#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/small_set.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

//! Note: TYamrWriter only supports tabular data.
class TYamredDsvWriter
    : public TFormatsConsumerBase
{
public:
    explicit TYamredDsvWriter(
        TOutputStream* stream,
        TYamredDsvFormatConfigPtr config = New<TYamredDsvFormatConfig>());

    ~TYamredDsvWriter();

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
    TYamredDsvFormatConfigPtr Config;

    TStringBuf Key;
    DECLARE_ENUM(EState,
        (None)
        (ExpectingValue)
    );
    EState State;

    TBlobOutput ValueBuffer;

    bool IsValueEmpty;
    bool AllowBeginMap;

    bool ExpectTableIndex;

    TDsvTable Table;

    TSmallSet<TStringBuf, 4> KeyColumnNames;
    TSmallSet<TStringBuf, 4> SubkeyColumnNames;

    // For small data sizes, set and map are faster than hash set and hash map.
    typedef std::map<TStringBuf, TStringBuf> TDictionary;

    TDictionary KeyFields;
    i32 KeyCount;

    TDictionary SubkeyFields;
    i32 SubkeyCount;

    void RememberValue(const TStringBuf& value);

    void WriteRow();
    void WriteYamrField(
        const std::vector<Stroka>& columnNames,
        const TDictionary& fieldValues,
        i32 fieldCount);

    void EscapeAndWrite(
        TOutputStream* outputStream,
        const TStringBuf& string,
        bool inKey);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

