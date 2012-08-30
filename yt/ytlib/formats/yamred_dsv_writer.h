#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"

#include <ytlib/misc/blob_output.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

//! Note: #TYamrWriter supports only tabular data
class TYamredDsvWriter
    : public TFormatsConsumerBase
{
public:
    explicit TYamredDsvWriter(
        TOutputStream* stream,
        TYamredDsvFormatConfigPtr config = NULL);

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

    Stroka Key;
    DECLARE_ENUM(EState,
        (None)
        (ExpectingValue)
    );
    EState State;
    
    // On small amount of data set and map work faster
    // than hash set and hash map.
    std::set<Stroka> KeyColumnNames;
    std::set<Stroka> SubkeyColumnNames;

    std::map<Stroka, Stroka> KeyFields;
    std::map<Stroka, Stroka> SubkeyFields;

    TBlobOutput ValueBuffer;

    bool IsValueEmpty;
    bool AllowBeginMap;

    void RememberValue(const TStringBuf& value);

    void WriteRow();
    void WriteYamrField(
        const std::vector<Stroka>& columnNames,
        const std::map<Stroka, Stroka>& fieldValues);
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT

