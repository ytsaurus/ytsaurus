#pragma once

#include <contrib/ydb/core/kafka_proxy/actors/actors.h>
#include <contrib/ydb/core/kafka_proxy/kafka.h>
#include <contrib/ydb/core/kafka_proxy/kafka_messages.h>

namespace NKafka::NKafkaTransactions {
    template<class ResponseType, class RequestType>
    std::shared_ptr<ResponseType> BuildResponse(TMessagePtr<RequestType> request, EKafkaErrors errorCode);
} // namespace NKafka::NKafkaTransactions