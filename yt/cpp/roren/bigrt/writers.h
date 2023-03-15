#pragma once

#include "fwd.h"

#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/key_value.h>
#include <yt/cpp/roren/interface/transforms.h>

#include <bigrt/lib/writer/yt_queue/proto/config.pb.h>

#include <quality/user_sessions/rt/lib/writers/logbroker/types.h>

#include <library/cpp/yson/node/node.h>
#include <yt/yt/client/table_client/public.h>
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
/// @brief Transform writing raw bytes to yt queue.
///
/// Transform expects input of TKV:
///   - key is shard number,
///   - value is payload bytes.
TTransform<TKV<ui64, TString>, void> WriteRawYtQueue(NBigRT::TYtQueueWriterConfig config);

////////////////////////////////////////////////////////////////////////////////

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
TTransform<TLogbrokerData, void> WriteRawLogbroker(NUserSessions::NRT::TLogbrokerWriterConfig config);

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

    void ApplyTo(const TPCollection<NYT::TNode>& pCollection) const;
    void ApplyTo(const TPCollection<NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>>& pCollection) const;

private:
    template <class T>
    void ApplyTo(const TPCollection<T>& pCollection) const;
    TString YtPath_;
    NYT::NTableClient::TNameTablePtr NameTable_;
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
