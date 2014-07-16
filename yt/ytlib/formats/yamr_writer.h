#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "yamr_table.h"

#include <ytlib/table_client/public.h>

#include <core/misc/blob_output.h>
#include <core/misc/nullable.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

//! Note: only tabular format is supported.
class TYamrConsumer
    : public virtual TFormatsConsumerBase
{
public:
    explicit TYamrConsumer(
        TOutputStream* stream,
        TYamrFormatConfigPtr config = New<TYamrFormatConfig>());

    ~TYamrConsumer();

    // IYsonConsumer overrides.
    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
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
    DECLARE_ENUM(EState,
        (None)
        (ExpectColumnName)
        (ExpectValue)
        (ExpectAttributeName)
        (ExpectAttributeValue)
        (ExpectEndAttributes)
        (ExpectEntity)
    );

    DECLARE_ENUM(EValueType,
        (ExpectKey)
        (ExpectSubkey)
        (ExpectValue)
        (ExpectUnknown)
    );

    TOutputStream* Stream;
    TYamrFormatConfigPtr Config;

    TNullable<TStringBuf> Key;
    TNullable<TStringBuf> Subkey;
    TNullable<TStringBuf> Value;

    TYamrTable Table;

    EState State;
    EValueType ValueType;
    NTableClient::EControlAttribute ControlAttribute;

    // To store Int64 and Double values converted to strings.
    std::vector<Stroka> StringStorage_;

    void WriteRow();
    void WriteInLenvalMode(const TStringBuf& value);

    void EscapeAndWrite(const TStringBuf& value, bool inKey);
    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
