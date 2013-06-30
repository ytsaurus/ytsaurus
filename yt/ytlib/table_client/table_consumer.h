#pragma once

#include "public.h"

#include <ytlib/yson/consumer.h>
#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/blob_range.h>
#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  For performance reasons we don't use ForwardingConsumer.
 */
class TTableConsumer
    : public NYson::IYsonConsumer
{
public:
    explicit TTableConsumer(ISyncWriterPtr writer);
    TTableConsumer(const std::vector<ISyncWriterPtr>& writers, int tableIndex);

private:
    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnIntegerScalar(i64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& name) override;
    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;

    void ThrowMapExpected();
    void ThrowInvalidControlAttribute(const Stroka& whatsWrong);

    virtual void OnEndList() override;
    virtual void OnEndAttributes() override;
    virtual void OnRaw(const TStringBuf& yson, NYson::EYsonType type) override;

    DECLARE_ENUM(EControlState,
        (None)
        (ExpectName)
        (ExpectValue)
        (ExpectEndAttributes)
        (ExpectEntity)
    );

    EControlState ControlState;
    EControlAttribute ControlAttribute;

    int CurrentTableIndex;
    std::vector<ISyncWriterPtr> Writers;
    ISyncWriterPtr Writer;

    int Depth;

    //! Keeps the current row data.
    TBlobOutput RowBuffer;

    //! |(endColumn, endValue)| offsets in #RowBuffer.
    std::vector<size_t> Offsets;

    NYson::TYsonWriter ValueWriter;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
