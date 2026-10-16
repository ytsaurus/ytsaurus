#include "entity_builders.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NTesting {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

TMessageId MakeFreshMessageId()
{
    return TMessageId(ToString(TGuid::Create()));
}

void StampMeta(TMessageBuilder& builder)
{
    builder.SetMessageId(MakeFreshMessageId());
    builder.SetSystemTimestamp(TSystemTimestamp(1));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1));
    builder.SetEventTimestamp(TSystemTimestamp(1));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr DefaultTestKeySchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=key;type=uint64;sort_order=ascending}])")));
}

TInputMessageConstPtr MakeTestMessage(
    const TStreamId& streamId,
    const TKey& key,
    const TTableSchemaPtr& schema,
    const TMessageBuilder::TInitFunction& init)
{
    TMessageBuilder builder(streamId, schema);
    StampMeta(builder);
    if (init) {
        init(builder);
    }
    return New<TInputMessage>(builder.Finish(), key);
}

TMessage MakeTestRawMessage(
    const TStreamId& streamId,
    const TTableSchemaPtr& schema,
    const TMessageBuilder::TInitFunction& init)
{
    TMessageBuilder builder(streamId, schema);
    StampMeta(builder);
    if (init) {
        init(builder);
    }
    return builder.Finish();
}

TInputTimerConstPtr MakeTestTimer(
    const TKey& key,
    TSystemTimestamp triggerTimestamp,
    const TTableSchemaPtr& keySchema,
    const TStreamId& streamId)
{
    TTimer timer;
    timer.MessageId = MakeFreshMessageId();
    timer.SystemTimestamp = TSystemTimestamp(1);
    timer.AlignmentTimestamp = TSystemTimestamp(1);
    timer.EventTimestamp = TSystemTimestamp(1);
    timer.StreamId = streamId;
    timer.Key = key;
    timer.KeySchema = keySchema;
    timer.TriggerTimestamp = triggerTimestamp;
    return New<TInputTimer>(std::move(timer), keySchema);
}

TInputVisitConstPtr MakeTestVisit(
    const TKey& key,
    const TStreamId& streamId)
{
    TVisit visit;
    visit.MessageId = MakeFreshMessageId();
    visit.SystemTimestamp = TSystemTimestamp(1);
    visit.AlignmentTimestamp = TSystemTimestamp(1);
    visit.EventTimestamp = TSystemTimestamp(1);
    visit.StreamId = streamId;
    visit.Key = key;
    return New<TInputVisit>(std::move(visit));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
