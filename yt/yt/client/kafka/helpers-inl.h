#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NKafka {

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TResponse>
concept CHasErrorCode = requires (const TResponse& item) {
    { item.ErrorCode } -> std::convertible_to<EErrorCode>;
};

template <class TResponse>
concept CHasTopics = requires (const TResponse& item) { item.Topics; };

template <class TResponse>
concept CHasPartitions = requires (const TResponse& item) { item.Partitions; };

template <class TResponse>
concept CHasResponses = requires (const TResponse& item) { item.Responses; };

template <class TResponse>
concept CHasPartitionResponses = requires (const TResponse& item) { item.PartitionResponses; };

template <class TResponse>
concept CHasNested =
    CHasTopics<TResponse> || CHasPartitions<TResponse> || CHasResponses<TResponse> || CHasPartitionResponses<TResponse>;

template <typename TResponse>
std::vector<EErrorCode> CollectNestedErrorCodes(const std::vector<TResponse>& items)
{
    std::vector<EErrorCode> codes;
    for (const auto& item : items) {
        auto nested = CollectErrorCodes(item);
        codes.insert(codes.end(), nested.begin(), nested.end());
    }
    return codes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <typename TResponse>
    requires (NDetail::CHasErrorCode<TResponse> && !NDetail::CHasNested<TResponse>)
std::vector<EErrorCode> CollectErrorCodes(const TResponse& item)
{
    if (item.ErrorCode != EErrorCode::None) {
        return {item.ErrorCode};
    }
    return {};
}

template <typename TResponse>
    requires (NDetail::CHasTopics<TResponse> && !NDetail::CHasErrorCode<TResponse>)
std::vector<EErrorCode> CollectErrorCodes(const TResponse& item)
{
    return NDetail::CollectNestedErrorCodes(item.Topics);
}

template <typename TResponse>
    requires NDetail::CHasPartitions<TResponse>
std::vector<EErrorCode> CollectErrorCodes(const TResponse& item)
{
    auto codes = NDetail::CollectNestedErrorCodes(item.Partitions);
    if constexpr (NDetail::CHasErrorCode<TResponse>) {
        if (item.ErrorCode != EErrorCode::None) {
            codes.push_back(item.ErrorCode);
        }
    }
    return codes;
}

template <typename TResponse>
    requires (NDetail::CHasResponses<TResponse> && !NDetail::CHasErrorCode<TResponse>)
std::vector<EErrorCode> CollectErrorCodes(const TResponse& item)
{
    return NDetail::CollectNestedErrorCodes(item.Responses);
}

template <typename TResponse>
    requires (NDetail::CHasPartitionResponses<TResponse> && !NDetail::CHasErrorCode<TResponse>)
std::vector<EErrorCode> CollectErrorCodes(const TResponse& item)
{
    return NDetail::CollectNestedErrorCodes(item.PartitionResponses);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka
