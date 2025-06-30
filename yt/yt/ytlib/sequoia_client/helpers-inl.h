#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> WrapSequoiaRetriableError(
    std::conditional_t<std::is_void_v<T>, const TError&, TErrorOr<T>&&> result)
{
    return TError(EErrorCode::SequoiaRetriableError, "Sequoia retriable error")
        << std::forward<decltype(result)>(result);
}

template <class T>
TErrorOr<T> MaybeWrapSequoiaRetriableError(
    std::conditional_t<std::is_void_v<T>, const TError&, TErrorOr<T>&&> result)
{
    if (!result.IsOK() &&
        !result.FindMatching(EErrorCode::SequoiaRetriableError) &&
        IsRetriableSequoiaError(result))
    {
        return WrapSequoiaRetriableError<T>(std::forward<decltype(result)>(result));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <class TOnReplica>
void ParseChunkReplicas(
    NYson::TYsonStringBuf replicasYson,
    const TOnReplica& onReplica)
{
    using namespace NYson;

    TMemoryInput input(replicasYson.AsStringBuf().data(), replicasYson.AsStringBuf().size());
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);

    cursor.ParseList([&] (NYson::TYsonPullParserCursor* cursor) {
        TParsedChunkReplica parsedReplica;

        auto consume = [&] (EYsonItemType type, const auto& fillField) {
            const auto& current = cursor->GetCurrent();
            if (current.GetType() != type) {
                THROW_ERROR_EXCEPTION("Invalid YSON item type while parsing Sequoia replicas: expected %Qlv, actual %Qlv",
                    type,
                    current.GetType());
            }
            fillField(current);
            cursor->Next();
        };

        consume(EYsonItemType::BeginList, [] (const TYsonItem&) {});
        consume(EYsonItemType::Uint64Value, [&] (const TYsonItem& current) {
            parsedReplica.LocationIndex = NNodeTrackerClient::TChunkLocationIndex(current.UncheckedAsUint64());
        });
        consume(EYsonItemType::Int64Value, [&] (const TYsonItem& current) {
            parsedReplica.ReplicaIndex = current.UncheckedAsInt64();
        });
        consume(EYsonItemType::Uint64Value, [&] (const TYsonItem& current) {
            parsedReplica.NodeId = NNodeTrackerClient::TNodeId(current.UncheckedAsUint64());
        });
        consume(EYsonItemType::EndList, [] (const TYsonItem&) {});

        onReplica(parsedReplica);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
