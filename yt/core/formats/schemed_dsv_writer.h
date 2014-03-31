#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "schemed_dsv_table.h"

#include <core/misc/blob_output.h>
#include <core/misc/nullable.h>

#include <ytlib/table_client/public.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

//! Note: #TSchemafulDsvWriter supports only tabular data
class TSchemafulDsvWriter
    : public virtual TFormatsConsumerBase
{
public:
    explicit TSchemafulDsvWriter(
        TOutputStream* stream,
        TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

    ~TSchemafulDsvWriter();

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
    TOutputStream* Stream_;
    TSchemafulDsvFormatConfigPtr Config_;

    TSchemafulDsvTable Table_;

    std::set<TStringBuf> Keys_;
    std::map<TStringBuf, TStringBuf> Values_;

    int ValueCount_;
    TStringBuf CurrentKey_;

    int TableIndex_;

    DECLARE_ENUM(EState,
        (None)
        (ExpectValue)
        (ExpectAttributeName)
        (ExpectAttributeValue)
        (ExpectEndAttributes)
        (ExpectEntity)
    );

    EState State_;

    NTableClient::EControlAttribute ControlAttribute_;

    void WriteRow();
    void EscapeAndWrite(const TStringBuf& value) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

