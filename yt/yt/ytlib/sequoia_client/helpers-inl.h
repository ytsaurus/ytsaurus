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

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> TransformSequoiaTransactionCommitError(
    std::conditional_t<std::is_void_v<T>, const TError&, TErrorOr<T>&&> error)
{
#define FOR_EACH_ERROR_CODE_TO_STRIP(process) \
    process(NTransactionClient::EErrorCode::AtomicTransactionCommitFailure) \
    process(NTransactionClient::EErrorCode::NativeTransactionCommitFailure) \
    process(NTransactionClient::EErrorCode::ParticipantFailedToPrepare) \
    process(NSequoiaClient::EErrorCode::TransactionActionFailedOnMasterCell)

#define XX(errorCode) TErrorCode(errorCode),

    static const std::vector<TErrorCode> ErrorCodesToStrip = {
        FOR_EACH_ERROR_CODE_TO_STRIP(XX)
    };

#define XX(errorCode) TErrorCode(errorCode),

    static const std::vector<TErrorCode> ErrorCodesToStripPlusSequoiaRetriableError = {
        FOR_EACH_ERROR_CODE_TO_STRIP(XX)
        NSequoiaClient::EErrorCode::SequoiaRetriableError
    };

#undef XX
#undef FOR_EACH_ERROR_CODE_TO_STRIP

    if (error.IsOK()) {
        return error;
    }

    auto retriable =
        error.FindMatching(EErrorCode::SequoiaRetriableError) ||
        IsRetriableSequoiaError(error);
    const std::vector<TErrorCode>& errorCodesToStrip = retriable
        ? ErrorCodesToStripPlusSequoiaRetriableError
        : ErrorCodesToStrip;

    std::vector<std::string> strippedMessages;
    auto* currentError = &error;
    while (std::find(errorCodesToStrip.begin(), errorCodesToStrip.end(), currentError->GetCode()) != errorCodesToStrip.end() &&
        std::ssize(currentError->InnerErrors()) == 1)
    {
        strippedMessages.push_back(currentError->GetMessage());
        currentError = &currentError->InnerErrors().front();
    }

    auto transformedError = *currentError;
    transformedError
        <<= TErrorAttribute("stripped_error_messages", strippedMessages);

    if (retriable) {
        transformedError = WrapSequoiaRetriableError<T>(std::move(transformedError));
    }

    return transformedError;
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
