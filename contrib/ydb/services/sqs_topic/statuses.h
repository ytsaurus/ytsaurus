#pragma once
#include "error.h"
#include <contrib/ydb/core/persqueue/public/describer/describer.h>
#include <contrib/ydb/core/protos/sqs.pb.h>
#include <util/generic/string.h>

namespace NKikimr::NSqsTopic::V1 {

    struct TMappedDescriberError {
        NKikimr::NPQ::NDescriber::EStatus DescriberStatus{};
        TMaybe<NSQS::TError> Error;
    };

    TMappedDescriberError MapDescriberStatus(const TString& topicPath, NKikimr::NPQ::NDescriber::EStatus status);
} // namespace NKikimr::NSqsTopic::V1
