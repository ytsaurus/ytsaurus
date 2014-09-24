#include "stdafx.h"
#include "journal_commands.h"
#include "config.h"

#include <core/misc/blob_output.h>

#include <core/concurrency/scheduler.h>

#include <core/yson/consumer.h>

#include <core/ytree/fluent.h>

#include <ytlib/formats/format.h>
#include <ytlib/formats/parser.h>

#include <ytlib/api/journal_reader.h>
#include <ytlib/api/journal_writer.h>

namespace NYT {
namespace NDriver {

using namespace NYson;
using namespace NYTree;
using namespace NFormats;
using namespace NConcurrency;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TReadJournalCommand::DoExecute()
{
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->JournalReader,
        Request_->GetOptions());

    TJournalReaderOptions options;
    auto lowerLimit = Request_->Path.GetLowerLimit();
    i64 lowerRowLImit = 0;
    if (lowerLimit.HasRowIndex()) {
        options.FirstRowIndex = lowerRowLImit = lowerLimit.GetRowIndex();
    }
    auto upperLimit = Request_->Path.GetUpperLimit();
    if (upperLimit.HasRowIndex()) {
        options.RowCount = upperLimit.GetRowIndex() - lowerRowLImit;
    }
    SetTransactionalOptions(&options);

    auto reader = Context_->GetClient()->CreateJournalReader(
        Request_->Path.GetPath(),
        options,
        config);
    
    {
        auto error = WaitFor(reader->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    auto output = Context_->Request().OutputStream;

    // TODO(babenko): provide custom allocation tag
    TBlobOutput buffer;
    auto flushBuffer = [&] () {
        auto result = WaitFor(output->Write(buffer.Begin(), buffer.Size()));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
        buffer.Clear();
    };

    auto format = Context_->GetOutputFormat();
    auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &buffer);

    while (true) {
        auto rowsOrError = WaitFor(reader->Read());
        THROW_ERROR_EXCEPTION_IF_FAILED(rowsOrError);
        const auto& rows = rowsOrError.Value();

        if (rows.empty())
            break;

        for (auto row : rows) {
            BuildYsonListFluently(consumer.get())
                .Item().BeginMap()
                    .Item("data").Value(TStringBuf(row.Begin(), row.Size()))
                .EndMap();
        }

        if (buffer.Size() > Context_->GetConfig()->ReadBufferSize) {
            flushBuffer();
        }
    }

    if (buffer.Size() > 0) {
        flushBuffer();
    }
}

//////////////////////////////////////////////////////////////////////////////////

class TJournalConsumer
    : public TYsonConsumerBase
{
public:
    explicit TJournalConsumer(IJournalWriterPtr writer)
        : Writer_(writer)
    { }

private:
    IJournalWriterPtr Writer_;

    DECLARE_ENUM(EState,
        (Root)
        (AtItem)
        (InsideMap)
        (AtData)
    );

    EState State_ = EState::Root;


    virtual void OnStringScalar(const TStringBuf& value) override
    {
        if (State_ != EState::AtData) {
            ThrowMalformedData();
        }

        std::vector<TSharedRef> rows;
        rows.push_back(TSharedRef::FromString(Stroka(value)));
        
        auto error = Writer_->Write(rows).Get(); //WaitFor(Writer_->Write(rows));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);

        State_ = EState::InsideMap;
    }

    virtual void OnInt64Scalar(i64 /*value*/) override
    {
        ThrowMalformedData();
    }

    virtual void OnUint64Scalar(ui64 /*value*/) override
    {
        ThrowMalformedData();
    }

    virtual void OnDoubleScalar(double /*value*/) override
    {
        ThrowMalformedData();
    }

    virtual void OnBooleanScalar(bool /*value*/) override
    {
        ThrowMalformedData();
    }

    virtual void OnEntity() override
    {
        ThrowMalformedData();
    }

    virtual void OnBeginList() override
    {
        ThrowMalformedData();
    }

    virtual void OnListItem() override
    {
        if (State_ != EState::Root) {
            ThrowMalformedData();
        }
        State_ = EState::AtItem;
    }

    virtual void OnEndList() override
    {
        YUNREACHABLE();
    }

    virtual void OnBeginMap() override
    {
        if (State_ != EState::AtItem) {
            ThrowMalformedData();
        }
        State_ = EState::InsideMap;
    }

    virtual void OnKeyedItem(const TStringBuf& key) override
    {
        if (State_ != EState::InsideMap) {
            ThrowMalformedData();
        }
        if (key != STRINGBUF("data")) {
            ThrowMalformedData();
        }
        State_ = EState::AtData;
    }

    virtual void OnEndMap() override
    {
        if (State_ != EState::InsideMap) {
            ThrowMalformedData();
        }
        State_ = EState::Root;
    }

    virtual void OnBeginAttributes() override
    {
        ThrowMalformedData();
    }

    virtual void OnEndAttributes() override
    {
        YUNREACHABLE();
    }


    void ThrowMalformedData()
    {
        THROW_ERROR_EXCEPTION("Malformed journal data");
    }

};

void TWriteJournalCommand::DoExecute()
{
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->JournalWriter,
        Request_->GetOptions());

    TJournalWriterOptions options;
    SetTransactionalOptions(&options);

    auto writer = Context_->GetClient()->CreateJournalWriter(
        Request_->Path.GetPath(),
        options,
        config);

    {
        auto error = WaitFor(writer->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    TJournalConsumer consumer(writer);

    auto format = Context_->GetInputFormat();
    auto parser = CreateParserForFormat(format, EDataType::Tabular, &consumer);

    struct TWriteBufferTag { };
    auto buffer = TSharedRef::Allocate<TWriteBufferTag>(Context_->GetConfig()->WriteBufferSize);

    auto input = Context_->Request().InputStream;

    while (true) {
        auto bytesRead = WaitFor(input->Read(buffer.Begin(), buffer.Size()));
        THROW_ERROR_EXCEPTION_IF_FAILED(bytesRead);

        if (bytesRead.Value() == 0)
            break;

        parser->Read(TStringBuf(buffer.Begin(), bytesRead.Value()));
    }

    parser->Finish();

    {
        auto error = WaitFor(writer->Close());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
