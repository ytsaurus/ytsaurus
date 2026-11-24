#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/core/actions/public.h>

#include <ydb-cpp-sdk/client/topic/client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline constexpr ui32 YdbCommitToPastIssueCode = 500025;

////////////////////////////////////////////////////////////////////////////////

NYdb::NTopic::ECodec ParseYdbCodec(const std::string& codec);

std::string DecompressYdbTopicMessage(const std::string& data, NYdb::NTopic::ECodec codecId);

NYdb::IExecutor::TPtr CreateThreadPoolExecutorAdapter(IInvokerPtr invoker);

//! Takes exclusive ownership of |readSession| and destroys it outside job executors.
std::shared_ptr<NYdb::NTopic::IReadSession> WrapReadSessionForAsyncDestruction(
    std::shared_ptr<NYdb::NTopic::IReadSession> readSession,
    IResourcePtr driver);

////////////////////////////////////////////////////////////////////////////////

TKey GenerateYdbTopicKey(
    std::string_view sourceIdentity,
    const std::string& endpoint,
    int partitionIndex);
std::pair<std::string, int> UnpackYdbTopicKey(const TKey& key);
//! Groups keys by endpoint; keys of retiring partitions may predate the current layout and share a fallback group.
std::string GetYdbTopicKeyGroup(const TKey& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
