#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "dsv_table.h"

#include <ytlib/table_client/public.h>

#include <core/misc/enum.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TDsvWriterBase
    : public virtual TFormatsConsumerBase
{
public:
    explicit TDsvWriterBase(
        TOutputStream* stream,
        TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

protected:
    TOutputStream* Stream;
    TDsvFormatConfigPtr Config;

    TDsvTable Table;

    void EscapeAndWrite(const TStringBuf& string, bool inKey);

};

////////////////////////////////////////////////////////////////////////////////

class TDsvTabularWriter
    : public TDsvWriterBase
{
public:
    explicit TDsvTabularWriter(
        TOutputStream* stream,
        TDsvFormatConfigPtr config = New<TDsvFormatConfig>());
    ~TDsvTabularWriter();

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
    DECLARE_ENUM(EState,
        (None)
        (ExpectAttributeName)
        (ExpectAttributeValue)
        (ExpectEntity)
        (ExpectColumnName)
        (ExpectFirstColumnName)
        (ExpectColumnValue)
    );

    NTableClient::EControlAttribute ControlAttribute;

    EState State;
    int TableIndex;

};

////////////////////////////////////////////////////////////////////////////////

// YsonNode is written as follows:
//  * Each element of list is ended with RecordSeparator
//  * Items in map are separated with FieldSeparator
//  * Key and Values in map are separated with KeyValueSeparator
class TDsvNodeWriter
    : public TDsvWriterBase
{
public:
    explicit TDsvNodeWriter(
        TOutputStream* stream,
        TDsvFormatConfigPtr config = New<TDsvFormatConfig>());
    ~TDsvNodeWriter();

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
    bool AllowBeginList;
    bool AllowBeginMap;

    bool BeforeFirstMapItem;
    bool BeforeFirstListItem;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
