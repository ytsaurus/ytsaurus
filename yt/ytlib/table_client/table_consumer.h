#pragma once

#include "public.h"

#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/blob_range.h>
#include <ytlib/misc/nullable.h>

#include <ytlib/yson/consumer.h>
#include <ytlib/yson/writer.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  For performance reasons we don't use TForwardingYsonConsumer.
 */
class TTableConsumer
    : public NYson::IYsonConsumer
{
public:
    template<class TWriter>
    explicit TTableConsumer(TWriter writer)
        : ControlState(EControlState::None)
        , CurrentTableIndex(0)
        , Writer(writer)
        , Depth(0)
        , ValueWriter(&RowBuffer)
    {
        Writers.push_back(writer);
    }

    template<class TWriter>
    TTableConsumer(const std::vector<TWriter>& writers, int tableIndex)
        : ControlState(EControlState::None)
        , CurrentTableIndex(tableIndex)
        , Writers(writers.begin(), writers.end())
        , Writer(Writers[CurrentTableIndex])
        , Depth(0)
        , ValueWriter(&RowBuffer)
    { }

    virtual void OnKeyedItem(const TStringBuf& name) override;
    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnIntegerScalar(i64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;
    virtual void OnRaw(const TStringBuf& yson, NYson::EYsonType type) override;

private:
    void ThrowError(const Stroka& message) const;
    void ThrowMapExpected() const;
    void ThrowEntityExpected() const;
    void ThrowInvalidControlAttribute(const Stroka& whatsWrong) const;

    DECLARE_ENUM(EControlState,
        (None)
        (ExpectControlAttributeName)
        (ExpectControlAttributeValue)
        (ExpectEndControlAttributes)
        (ExpectEntity)
    );

    EControlState ControlState;
    EControlAttribute ControlAttribute;

    int CurrentTableIndex;
    std::vector<IWriterBasePtr> Writers;
    IWriterBasePtr Writer;

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
