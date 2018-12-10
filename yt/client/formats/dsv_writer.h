#pragma once

#include "config.h"
#include "escape.h"
#include "helpers.h"
#include "public.h"
#include "schemaless_writer_adapter.h"

#include <yt/client/table_client/public.h>

#include <yt/core/misc/enum.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TDsvWriterBase
{
public:
    explicit TDsvWriterBase(TDsvFormatConfigPtr config);

protected:
    const TDsvFormatConfigPtr Config_;
    TEscapeTable KeyEscapeTable_;
    TEscapeTable ValueEscapeTable_;
};

////////////////////////////////////////////////////////////////////////////////

// YsonNode is written as follows:
//  * Each element of list is ended with RecordSeparator
//  * Items in map are separated with FieldSeparator
//  * Key and Values in map are separated with KeyValueSeparator
class TDsvNodeConsumer
    : public TDsvWriterBase
    , public TFormatsConsumerBase
{
public:
    explicit TDsvNodeConsumer(
        IOutputStream* stream,
        TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

    // IYsonConsumer overrides.
    virtual void OnStringScalar(TStringBuf value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(TStringBuf key) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

private:
    IOutputStream* const Stream_;

    bool AllowBeginList_ = true;
    bool AllowBeginMap_ = true;

    bool BeforeFirstMapItem_ = true;
    bool BeforeFirstListItem_ = true;

};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForDsv(
    TDsvFormatConfigPtr config,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int /* keyColumnCount */);

ISchemalessFormatWriterPtr CreateSchemalessWriterForDsv(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int /* keyColumnCount */);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
