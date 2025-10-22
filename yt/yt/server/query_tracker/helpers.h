#pragma once

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/public.h>

#include <yt/yt/core/compression/codec.h>
#include <yt/yt/core/misc/common.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MaxDyntableStringSize = 16_MB;

////////////////////////////////////////////////////////////////////////////////

std::string BuildFilterFactors(const std::string& query, const NYson::TYsonString& annotations, const NYson::TYsonString& accessControlObjects);

////////////////////////////////////////////////////////////////////////////////

template <typename TPartial>
NApi::TQuery PartialRecordToQuery(const TPartial& partialRecord);

THashSet<std::string> GetUserSubjects(const std::string& user, const NApi::IClientPtr& client);
void ConvertAcoToOldFormat(NApi::TQuery& query);

////////////////////////////////////////////////////////////////////////////////

TString Compress(const TString& data, std::optional<ui64> maxCompressedStringSize = std::nullopt, int quality = 9);
TString Decompress(const std::string& data);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
