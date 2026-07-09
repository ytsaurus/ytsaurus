#pragma once

#include "public.h"

#include "payload.h"
#include "payload_validation.h"

#include <yt/yt/flow/library/cpp/common/proto/message.pb.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TMessageBuilder;

struct TMessageMeta
{
    TMessageId MessageId;

    // System timestamp, used for calculating system watermark and cleaning deduplication data.
    TSystemTimestamp SystemTimestamp;

    // Timestamp for aligning progress of processing messages of one stream.
    // Normally it is the instant of persisting this message in the source or in output_messages table.
    TSystemTimestamp AlignmentTimestamp;

    // Event timestamp, used for calculating event watermark.
    TSystemTimestamp EventTimestamp;

    TStreamId StreamId;
};

//! Message class.
//! Class has externalized yson serializer.
struct TMessage
    : public TMessageMeta
{
    //! Payload of message / user data.
    //! Invariant: index of value is equal to value.Id in this unversioned row.
    //! And index of value corresponds to column number in `PayloadSchema`.
    TPayload Payload;
    NTableClient::TTableSchemaPtr PayloadSchema;
};

void FormatValue(TStringBuilderBase* builder, const TMessage& message, TStringBuf /*spec*/);

i64 GetMessageMetaByteSize(const TMessageMeta& message);
i64 GetMessageByteSize(const TMessage& message);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TMessage* protoMessage,
    const TMessage& message,
    const TStreamSpecsPtr& specStorage);

void FromProto(
    TMessage* message,
    const NProto::TMessage& protoMessage,
    const TStreamSpecsPtr& specStorage);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TMessage& message, int columnId);
// Inefficient
NTableClient::TUnversionedValue GetColumn(const TMessage& message, TStringBuf columnName);

template <class T>
T GetColumnValue(const TMessage& message, int columnId);

// Inefficient
template <class T>
T GetColumnValue(const TMessage& message, TStringBuf columnName);

////////////////////////////////////////////////////////////////////////////////

class TMessageBuilder
{
public:
    using TInitFunction = std::function<void(TMessageBuilder& builder)>;

    TMessageBuilder(
        TStreamId streamId,
        NTableClient::TTableSchemaPtr schema,
        TInitFunction init = {});

    void SetMessageId(TMessageId messageId);
    void SetSystemTimestamp(TSystemTimestamp systemTimestamp);
    void SetAlignmentTimestamp(TSystemTimestamp systemTimestamp);
    void SetEventTimestamp(TSystemTimestamp eventTimestamp);

    TPayloadBuilder& Payload();

    const NTableClient::TTableSchemaPtr& GetSchema() const;
    TMessage Finish();

    void Reset();

private:
    const TStreamId StreamId_;
    const TInitFunction Init_;
    TPayloadBuilder PayloadBuilder_;
    TMessage CurrentMessage_;
};

////////////////////////////////////////////////////////////////////////////////

TSystemTimestamp GetOrderingTimestamp(
    const TMessage& message,
    const TInputOrderingSpecPtr& orderingSpec);

////////////////////////////////////////////////////////////////////////////////

//! Maximum adequate value for SystemTimestamp, AlignmentTimestamp and EventTimestamp.
constexpr TSystemTimestamp MaxAdequateTimestamp(4102434000); // 2100-01-01.

////////////////////////////////////////////////////////////////////////////////

//! Validates the common meta (message id, timestamps, stream id) shared by
//! messages, timers and visits. Throws on the first problem.
void ValidateMessageMeta(const TMessageMeta& meta);

//! Validates message meta and payload.
void ValidateMessage(const TMessage& message, const TValidatePayloadOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

//! Common base for objects carrying message meta.
struct TMessageMetaOwner
    : public TRefCounted
{
    virtual const TMessageMeta& GetMeta() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Validated input message with computed key and size.
//! Consider it as immutable and pass it as TInputMessageConstPtr.
struct TInputMessage
    : public TMessageMetaOwner
    , public TMessage
{
    TKey Key;
    i64 ByteSize;

    TInputMessage(TMessage&& message, TKey key);

    const TMessageMeta& GetMeta() const final;
};

DEFINE_REFCOUNTED_TYPE(TInputMessage);

void FormatValue(TStringBuilderBase* builder, const TInputMessageConstPtr& message, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TInputMessageConstPtr& message, int columnId);
// Inefficient
NTableClient::TUnversionedValue GetColumn(const TInputMessageConstPtr& message, TStringBuf columnName);

template <class T>
T GetColumnValue(const TInputMessageConstPtr& message, int columnId);

// Inefficient
template <class T>
T GetColumnValue(const TInputMessageConstPtr& message, TStringBuf columnName);

////////////////////////////////////////////////////////////////////////////////

//! Output message with computed size.
//! Consider it as immutable and pass it as TOutputMessageConstPtr.
//! The ctor validates meta/payload and the schema against the stream spec.
struct TOutputMessage
    : public TMessageMetaOwner
    , public TMessage
{
    i64 ByteSize;

    TOutputMessage(TMessage&& message, const TComputationStreamSpecStoragePtr& specStorage);

    const TMessageMeta& GetMeta() const final;
};

DEFINE_REFCOUNTED_TYPE(TOutputMessage);

////////////////////////////////////////////////////////////////////////////////

TMessageId GenerateOrderedMessageId(const TUniqueSeqNo& uniqueSeqNo, const TStreamId& streamId, const TStringBuf& offset);
TMessageId GenerateOrderedMessageId(const TUniqueSeqNo& uniqueSeqNo, const TStreamId& streamId, const TStringBuf& offset, const TStringBuf& secondaryOffset);
TMessageId GenerateInheritedMessageId(const TMessageId& sourceMessageId, const TStreamId& streamId, const TStringBuf& offset);

////////////////////////////////////////////////////////////////////////////////

//! For using TInputMessageConstPtr/TOutputMessageConstPtr as key in hash table instead of TMessageId.
struct TMessageHashMapOpsByMessageId
    : public THash<TMessageId>
    , public std::equal_to<TMessageId>
{
    using is_transparent = void;
    using THash<TMessageId>::operator();
    using std::equal_to<TMessageId>::operator();

    const TMessageId& GetMessageId(const TMessageId& value) const;

    template <class T>
        requires(std::is_base_of_v<TMessageMetaOwner, T>)
    const TMessageId& GetMessageId(const TIntrusivePtr<T>& value) const;

    template <class TLeft, class TRight>
    bool operator()(const TLeft& left, const TRight& right) const;

    template <typename T>
    size_t operator()(const T& value) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define MESSAGE_INL_H_
#include "message-inl.h"
#undef MESSAGE_INL_H_
