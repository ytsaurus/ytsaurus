#include "visit.h"

#include "key.h"
#include "message.h"

namespace NYT::NFlow {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static_assert(std::is_nothrow_move_constructible<TVisit>::value, "TVisit move constructor must be noexcept");

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TVisit& visit, TStringBuf /*spec*/)
{
    builder->AppendFormat("{MessageId: %v, SystemTimestamp: %v, EventTimestamp: %v, StreamId: %v, Key: %v}",
        visit.MessageId,
        visit.SystemTimestamp,
        visit.EventTimestamp,
        visit.StreamId,
        visit.Key);
}

i64 GetVisitByteSize(const TVisit& visit)
{
    return GetMessageMetaByteSize(visit) - sizeof(TMessageMeta) + sizeof(visit) + visit.Key.Underlying().GetSpaceUsed();
}

void ValidateVisit(const TVisit& visit)
{
    ValidateMessageMeta(visit);
    if (visit.Key.Underlying().GetCount() == 0) {
        THROW_ERROR_EXCEPTION("visit.Key is empty");
    }
}

////////////////////////////////////////////////////////////////////////////////

TInputVisit::TInputVisit(TVisit&& visit)
    : TVisit(std::move(visit))
    , ByteSize(GetVisitByteSize(*this))
{
    try {
        ValidateVisit(*this);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to create visit")
            << TError(ex)
            << TErrorAttribute("message_id", MessageId.Underlying())
            << TErrorAttribute("stream_id", StreamId);
    }
}

const TMessageMeta& TInputVisit::GetMeta() const
{
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TInputVisitConstPtr& visit, TStringBuf spec)
{
    FormatValue(builder, static_cast<const TVisit&>(*visit), spec);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TVisit* protoVisit, const TVisit& visit)
{
    // VisitMeta fields.
    protoVisit->set_message_id(ToProto<TProtobufString>(visit.MessageId));
    protoVisit->set_system_timestamp(ToProto(visit.SystemTimestamp));
    protoVisit->set_event_timestamp(ToProto(visit.EventTimestamp));
    protoVisit->set_stream_id(ToProto<TProtobufString>(visit.StreamId));
    // Visit fields.
    protoVisit->set_key(ToProto<TProtobufString>(visit.Key));
}

void FromProto(TVisit* visit, const NProto::TVisit& protoVisit)
{
    // VisitMeta fields.
    visit->MessageId = TMessageId(protoVisit.message_id());
    visit->SystemTimestamp = FromProto<TSystemTimestamp>(protoVisit.system_timestamp());
    visit->EventTimestamp = FromProto<TSystemTimestamp>(protoVisit.event_timestamp());
    visit->StreamId = FromProto<TStreamId>(protoVisit.stream_id());
    // Visit fields.
    visit->Key = FromProto<TKey>(protoVisit.key());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
