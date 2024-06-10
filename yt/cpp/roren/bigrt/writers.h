#pragma once

#include "fwd.h"

#include "transforms.h"
#include "destination_writer.h"

#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/key_value.h>
#include <yt/cpp/roren/interface/transforms.h>

#include <yt/yt/client/table_client/public.h>

#include <bigrt/lib/writer/yt_queue/proto/config.pb.h>
#include <bigrt/lib/writer/swift/writer.h>

#include <quality/user_sessions/rt/lib/writers/logbroker/types.h>

#include <library/cpp/framing/format.h>
#include <library/cpp/yson/node/node.h>

#include <util/system/types.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

void RorenEncode(IOutputStream* out, const NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>& rows);
void RorenDecode(IInputStream* in, NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>& result);

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Transform writing protobuf messages to yt queue.
///
/// Transform expects input of type `TKV<ui64, ProtoType>`.
///   - key is shard number,
///   - value is payload protobuf message.
///
/// Transform doesn't produce any output.
inline auto WriteYtQueue(NBigRT::TYtQueueWriterConfig config);

///
/// @brief Transform writing TNode to YT dynamic table
///
/// Transform expects input of type `TNode`.
/// TNode should have a Map type where is
///   - key is column name,
///   - value is column value
///
/// Transform doesn't produce any output.
inline auto WriteYtDynTable(TString ytPath, NYT::NTableClient::TNameTablePtr nameTable = {});

///
/// @brief Transform that packs proto messages and writes it to QYT.
///
/// Transform expects input of `TKV<ui64, Proto>`:
///   - key is shard number,
///   - value is proto message.
NPrivate::TPackAndWriteToYtQueueTransform PackAndWriteToYtQueue(
    NBigRT::TYtQueueWriterConfig config,
    NFraming::EFormat format,
    size_t maxSize = 8_MB);

///
/// @brief Transform writing raw bytes to yt queue.
///
/// If possible prefer to use higher level writers, e.g.:
///   - @ref NRoren::PackAndWriteToYtQueue
///
/// Transform expects input of TKV:
///   - key is shard number,
///   - value is payload bytes.
TTransform<TKV<ui64, TString>, void> WriteRawYtQueue(NBigRT::TYtQueueWriterConfig config);

////////////////////////////////////////////////////////////////////////////////
///
/// @brief Transform writing raw bytes to config destination.
///
/// Transform expects input of NRoren::TSerializedRow
TTransform<TSerializedRow, void> WriteToDestination(TString destinationName);

///
/// @brief Struct describing data to be written to logbroker.
///
/// @ref NRoren::WriteRawLogbroker
struct TLogbrokerData
{
    /// Logbroker partition index.
    ui64 Shard;

    /// Payload bytes.
    TString Value;

    ///
    /// @brief Logbroker sequence number, counter that is used by logbroker to deduplicate data.
    ///
    /// Users must provide monotonically increasing SeqNo. Usually it's computed from offsets of input data.
    ///
    /// @ref https://logbroker.yandex-team.ru/docs/concepts/resource_model#message-sequence-number
    NUserSessions::NRT::TSeqNo SeqNo;

    ///
    /// @brief Logbroker group index.
    ///
    /// @ref https://logbroker.yandex-team.ru/docs/concepts/resource_model#message-sequence-number
    NUserSessions::NRT::TGroupIndex GroupIndex;

    Y_SAVELOAD_DEFINE(Shard, Value, SeqNo, GroupIndex);
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Transform writing raw bytes to logbroker.
TTransform<TLogbrokerData, void> WriteRawLogbroker(NUserSessions::NRT::TLogbrokerWriterConfig config, TMaybe<NYT::NProfiling::TTagSet> profilerTags = Nothing());

////////////////////////////////////////////////////////////////////////////////

//
// IMPLEMENTATION
//

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TWriteYtQueueTransform
{
public:
    explicit TWriteYtQueueTransform(std::shared_ptr<const NBigRT::TYtQueueWriterConfig> config)
        : Config_(std::move(config))
    { }

    TString GetName() const
    {
        return "WriteYtQueue";
    }

    template <typename TProto>
    void ApplyTo(const TPCollection<TKV<ui64, TProto>>& pCollection) const
    {
        static_assert(std::is_base_of_v<::google::protobuf::Message, TProto>);

        pCollection | ParDo([] (const TKV<ui64, TProto>& inputRow) {
            const auto& [shard, message] = inputRow;
            return TKV{shard, message.SerializeAsString()};
        }) | WriteRawYtQueue(*Config_);
    }

private:
    std::shared_ptr<const NBigRT::TYtQueueWriterConfig> Config_;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteYtDynTableTransform
{
public:
    explicit TWriteYtDynTableTransform(TString ytPath, NYT::NTableClient::TNameTablePtr nameTable)
        : YtPath_(std::move(ytPath))
        , NameTable_(std::move(nameTable))
    { }

    TString GetName() const
    {
        return "WriteYtDynTable";
    }

    void ApplyTo(const TPCollection<NYT::TNode>& pCollection) const;
    void ApplyTo(const TPCollection<NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>>& pCollection) const;

private:
    template <class T>
    void ApplyTo(const TPCollection<T>& pCollection) const;
    TString YtPath_;
    NYT::NTableClient::TNameTablePtr NameTable_;
};

////////////////////////////////////////////////////////////////////////////////

class TPackAndWriteToYtQueueTransform
{
public:
    TPackAndWriteToYtQueueTransform(NBigRT::TYtQueueWriterConfig config, NFraming::EFormat format, size_t maxSize)
        : Config_(std::move(config))
        , Format_(format)
        , MaxSize_(maxSize)
    { }

    TString GetName() const
    {
        return "PackAndWriteToYtQueue";
    }

    template <typename TProto>
    void ApplyTo(const TPCollection<TKV<ui64, TProto>>& pCollection) const
    {
        static_assert(std::is_base_of_v<::google::protobuf::Message, TProto>);
        pCollection
            | PackFramedMessagesPerKeyParDo(Format_, MaxSize_)
            | WriteRawYtQueue(Config_);
    }

private:
    NBigRT::TYtQueueWriterConfig Config_;
    NFraming::EFormat Format_;
    size_t MaxSize_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

inline auto WriteYtQueue(NBigRT::TYtQueueWriterConfig config)
{
    return NPrivate::TWriteYtQueueTransform{std::make_shared<const NBigRT::TYtQueueWriterConfig>(std::move(config))};
}

inline auto WriteYtDynTable(TString ytPath, NYT::NTableClient::TNameTablePtr nameTable)
{
    return NPrivate::TWriteYtDynTableTransform{std::move(ytPath), std::move(nameTable)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
