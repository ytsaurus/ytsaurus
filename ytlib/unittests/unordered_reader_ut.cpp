#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/table_client/unordered_schemaful_reader.h>

#include <yt/ytlib/table_client/unordered_schemaful_reader.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/actions/future.h>

namespace NYT {
namespace {

using namespace NTableClient;
using namespace NConcurrency;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TUnorderedReaderTest
    : public ::testing::Test
{
public:

};

struct SchemafulReaderMock
    : public ISchemafulReader
{
    TPromise<void> ReadyEvent = NewPromise<void>();

    virtual bool Read(std::vector<TUnversionedRow>* rows)
    {
        rows->clear();
        return !ReadyEvent.IsSet();
    }

    virtual TFuture<void> GetReadyEvent()
    {
        return ReadyEvent;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return TDataStatistics();
    }

    virtual NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return NChunkClient::TCodecStatistics();
    }
};

TEST_F(TUnorderedReaderTest, Simple)
{
    auto reader1 = New<SchemafulReaderMock>();
    auto reader2 = New<SchemafulReaderMock>();

    auto subqueryReaderCreator = [&, index = 0] () mutable -> ISchemafulReaderPtr {
        if (index == 0) {
            ++index;
            return reader1;
        } else if (index == 1) {
            ++index;
            return reader2;
        } else {
            return nullptr;
        }
    };

    auto mergingReader = CreateUnorderedSchemafulReader(subqueryReaderCreator, 2);

    std::vector<TUnversionedRow> rows;

    YCHECK(mergingReader->Read(&rows));

    reader1->ReadyEvent.Set(TError());
    reader2->ReadyEvent.Set(TError("Error"));

    YCHECK(mergingReader->GetReadyEvent().IsSet());
    YCHECK(mergingReader->GetReadyEvent().Get().IsOK());

    YCHECK(mergingReader->Read(&rows));
    YCHECK(mergingReader->GetReadyEvent().IsSet());
    YCHECK(mergingReader->GetReadyEvent().Get().GetMessage() == "Error");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
