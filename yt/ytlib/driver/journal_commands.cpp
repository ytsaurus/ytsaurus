#include "stdafx.h"
#include "journal_commands.h"
#include "config.h"

#include <core/misc/blob_output.h>

#include <core/concurrency/scheduler.h>

#include <core/yson/consumer.h>

#include <core/ytree/fluent.h>

#include <ytlib/chunk_client/read_limit.h>

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
using namespace NChunkClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TReadJournalCommand::DoExecute()
{
    auto checkLimit = [] (const TReadLimit& limit) {
        if (limit.HasKey()) {
            THROW_ERROR_EXCEPTION("Reading key range is not supported in journals");
        }
        if (limit.HasChunkIndex()) {
            THROW_ERROR_EXCEPTION("Reading chunk index range is not supported in journals");
        }
        if (limit.HasOffset()) {
            THROW_ERROR_EXCEPTION("Reading offset range is not supported in journals");
        }
    };

    auto path = Request_->Path.Normalize();

    if (path.GetRanges().size() > 1) {
        THROW_ERROR_EXCEPTION("Reading multiple ranges is not supported in journals");
    }

    TJournalReaderOptions options;
    if (path.GetRanges().size() == 1) {
        auto range = path.GetRanges()[0];

        checkLimit(range.LowerLimit());
        checkLimit(range.UpperLimit());

        if (range.LowerLimit().HasRowIndex()) {
            options.FirstRowIndex = range.LowerLimit().GetRowIndex();
        } else {
            options.FirstRowIndex = static_cast<i64>(0);
        }

        if (range.UpperLimit().HasRowIndex()) {
            options.RowCount = range.UpperLimit().GetRowIndex() - *options.FirstRowIndex;
            options.FirstRowIndex = range.LowerLimit().GetRowIndex();
        }
    }
    options.Config = UpdateYsonSerializable(
        Context_->GetConfig()->JournalReader,
        Request_->GetOptions());
    SetTransactionalOptions(&options);

    auto reader = Context_->GetClient()->CreateJournalReader(
        Request_->Path.GetPath(),
        options);

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
        const auto& rows = rowsOrError.ValueOrThrow();

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

DEFINE_ENUM(EJournalConsumerState,
    (Root)
    (AtItem)
    (InsideMap)
    (AtData)
);

class TJournalConsumer
    : public TYsonConsumerBase
{
public:
    explicit TJournalConsumer(IJournalWriterPtr writer)
        : Writer_(writer)
    { }

private:
    IJournalWriterPtr Writer_;

    EJournalConsumerState State_ = EJournalConsumerState::Root;


    virtual void OnStringScalar(const TStringBuf& value) override
    {
        if (State_ != EJournalConsumerState::AtData) {
            ThrowMalformedData();
        }

        std::vector<TSharedRef> rows;
        rows.push_back(TSharedRef::FromString(Stroka(value)));

        auto error = WaitFor(Writer_->Write(rows));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);

        State_ = EJournalConsumerState::InsideMap;
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
        if (State_ != EJournalConsumerState::Root) {
            ThrowMalformedData();
        }
        State_ = EJournalConsumerState::AtItem;
    }

    virtual void OnEndList() override
    {
        YUNREACHABLE();
    }

    virtual void OnBeginMap() override
    {
        if (State_ != EJournalConsumerState::AtItem) {
            ThrowMalformedData();
        }
        State_ = EJournalConsumerState::InsideMap;
    }

    virtual void OnKeyedItem(const TStringBuf& key) override
    {
        if (State_ != EJournalConsumerState::InsideMap) {
            ThrowMalformedData();
        }
        if (key != STRINGBUF("data")) {
            ThrowMalformedData();
        }
        State_ = EJournalConsumerState::AtData;
    }

    virtual void OnEndMap() override
    {
        if (State_ != EJournalConsumerState::InsideMap) {
            ThrowMalformedData();
        }
        State_ = EJournalConsumerState::Root;
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
    TJournalWriterOptions options;
    options.Config = UpdateYsonSerializable(
        Context_->GetConfig()->JournalWriter,
        Request_->GetOptions());
    SetTransactionalOptions(&options);

    auto writer = Context_->GetClient()->CreateJournalWriter(
        Request_->Path.GetPath(),
        options);

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
