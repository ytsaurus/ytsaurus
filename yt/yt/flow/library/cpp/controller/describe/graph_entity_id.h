#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/misc/identifier.h>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

// Stream id naming convention (used by describe):
//   localStreamId  — local name within a computation (TStreamId), in YSON `name`.
//   globalStreamId — global name, may include computation prefix (TStreamId).
//   graphEntityId  — kind prefix + globalStreamId (TGraphEntityId), in YSON `id`.
//
// By front requirements, all streams, sources and sinks must have globally-unique
// graph ids, so each kind gets a distinct prefix. The graph id is kept as a distinct
// type from TStreamId on purpose: mixing a graph node id with a domain id in maps and
// lookups becomes a compile error.
YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(TGraphEntityId);

inline constexpr std::string_view StreamPrefix = "stm-";
inline constexpr std::string_view SourcePrefix = "src-";
inline constexpr std::string_view SinkPrefix = "snk-";
inline constexpr std::string_view ComputationPrefix = "cmp-";

TGraphEntityId MakeStreamGraphId(const TStreamId& globalStreamId);
TGraphEntityId MakeSourceGraphId(const TStreamId& globalStreamId);
TGraphEntityId MakeSinkGraphId(const TSinkId& globalSinkId);
TGraphEntityId MakeComputationGraphId(const TComputationId& computationId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
