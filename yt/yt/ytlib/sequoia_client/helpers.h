#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct TSelectRowsQuery
{
    std::vector<TString> WhereConjuncts;
    std::vector<TString> OrderBy;
    std::optional<int> Limit;
};

////////////////////////////////////////////////////////////////////////////////

inline const char MangledPathSeparator = '\0';

////////////////////////////////////////////////////////////////////////////////

TMangledSequoiaPath MangleSequoiaPath(NYPath::TYPathBuf rawPath);

NYPath::TYPath DemangleSequoiaPath(const TMangledSequoiaPath& mangledPath);

//! Unescapes special characters.
TString ToStringLiteral(TStringBuf key);

////////////////////////////////////////////////////////////////////////////////

inline constexpr TErrorCode RetriableSequoiaErrorCodes[] = {
    NTabletClient::EErrorCode::TransactionLockConflict,
    NTabletClient::EErrorCode::BlockedRowWaitTimeout,
    NTabletClient::EErrorCode::NoSuchTablet,
    NTabletClient::EErrorCode::ChunkIsNotPreloaded,
};

bool IsRetriableSequoiaError(const TError& error);

void ThrowOnSequoiaReplicasError(const TError& error, const std::vector<TErrorCode>& retriableErrorCodes);

bool IsMethodShouldBeHandledByMaster(const std::string& method);

////////////////////////////////////////////////////////////////////////////////

// NB: We want to use ApplyUnique() almost everywhere but TFuture<void> doesn't
// have this method. So |void| is a special case.
template <class T>
TErrorOr<T> MaybeWrapSequoiaRetriableError(
    std::conditional_t<std::is_void_v<T>, const TError&, TErrorOr<T>&&> result);

////////////////////////////////////////////////////////////////////////////////

struct TParsedChunkReplica
{
    NNodeTrackerClient::TNodeId NodeId = NNodeTrackerClient::InvalidNodeId;
    int ReplicaIndex = NChunkClient::GenericChunkReplicaIndex;
    NNodeTrackerClient::TChunkLocationIndex LocationIndex = NNodeTrackerClient::InvalidChunkLocationIndex;
};

template <class TOnReplica>
void ParseChunkReplicas(
    NYson::TYsonStringBuf replicasYson,
    const TOnReplica& onReplica);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
