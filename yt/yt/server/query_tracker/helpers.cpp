#include "helpers.h"

#include <yt/yt/ytlib/query_tracker_client/helpers.h>
#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <library/cpp/streams/zstd/zstd.h>

#include <contrib/libs/pfr/include/pfr/tuple_size.hpp>

namespace NYT::NQueryTracker {

using namespace NApi;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NQueryTrackerClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

std::string BuildFilterFactors(const std::string& query, const TYsonString& annotations, const TYsonString& accessControlObjects)
{
    return Format("%v %v acos:%v",
        query,
        annotations ? annotations.ToString() : "",
        accessControlObjects ? FormatAcoList(accessControlObjects) : "");
}

////////////////////////////////////////////////////////////////////////////////

TQuery PartialRecordToQuery(const auto& partialRecord)
{
    static_assert(pfr::tuple_size<TQuery>::value == 18);
    static_assert(TActiveQueryDescriptor::FieldCount == 23);
    static_assert(TFinishedQueryDescriptor::FieldCount == 19);

    TQuery query;
    // Note that some of the fields are twice optional.
    // First time due to the fact that they are optional in the record,
    // and second time due to the extra optionality of any field in the partial record.
    // TODO(max42): coalesce the optionality of the fields in the partial record.
    query.Id = partialRecord.Key.QueryId;
    query.Engine = partialRecord.Engine;
    query.Query = partialRecord.Query;
    query.Files = partialRecord.Files.value_or(std::nullopt);
    query.StartTime = partialRecord.StartTime;
    query.FinishTime = partialRecord.FinishTime.value_or(std::nullopt);
    query.Settings = partialRecord.Settings.value_or(TYsonString());
    query.User = partialRecord.User;
    query.AccessControlObjects = partialRecord.AccessControlObjects.value_or(TYsonString(TString("[]")));
    query.State = partialRecord.State;
    query.ResultCount = partialRecord.ResultCount.value_or(std::nullopt);
    query.Progress = TYsonString(partialRecord.Progress ? Decompress(partialRecord.Progress.value()) : TString("{}"));
    query.Error = partialRecord.Error.value_or(std::nullopt);
    query.Annotations = partialRecord.Annotations.value_or(TYsonString());
    query.Secrets = partialRecord.Secrets.value_or(TYsonString(TString("[]")));
    query.IsIndexed = partialRecord.IsIndexed;

    IAttributeDictionaryPtr otherAttributes;
    auto fillIfPresent = [&] (const TString& key, const auto& value) {
        if (value) {
            if (!otherAttributes) {
                otherAttributes = CreateEphemeralAttributes();
            }
            otherAttributes->Set(key, *value);
        }
    };

    if constexpr (std::is_same_v<std::decay_t<decltype(partialRecord)>, TActiveQueryPartial>) {
        fillIfPresent("abort_request", partialRecord.AbortRequest.value_or(std::nullopt));
        fillIfPresent("incarnation", partialRecord.Incarnation);
        fillIfPresent("lease_transaction_id", partialRecord.LeaseTransactionId);
    }
    fillIfPresent("assigned_tracker", partialRecord.AssignedTracker);
    fillIfPresent("is_tutorial", partialRecord.IsTutorial);

    query.OtherAttributes = std::move(otherAttributes);

    return query;
}

template TQuery PartialRecordToQuery<TActiveQueryPartial>(const TActiveQueryPartial&);
template TQuery PartialRecordToQuery<TFinishedQueryPartial>(const TFinishedQueryPartial&);

////////////////////////////////////////////////////////////////////////////////

THashSet<std::string> GetUserSubjects(const std::string& user, const IClientPtr& client)
{
    // Get all subjects for the user.
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    options.SuccessStalenessBound = TDuration::Minutes(1);
    auto path = Format("//sys/users/%v/@member_of_closure", NYPath::ToYPathLiteral(user));
    auto userSubjectsOrError = WaitFor(client->GetNode(path, options));
    if (!userSubjectsOrError.IsOK()) {
        if (userSubjectsOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return {};
        }
        THROW_ERROR_EXCEPTION("Error while fetching user membership for the user %Qv", user)
            << userSubjectsOrError;
    }
    return ConvertTo<THashSet<std::string>>(userSubjectsOrError.Value());
}

void ConvertAcoToOldFormat(TQuery& query)
{
    auto accessControlObjectList = ConvertTo<std::optional<std::vector<TString>>>(query.AccessControlObjects);

    if (!accessControlObjectList || accessControlObjectList->empty()) {
        return;
    }

    if (accessControlObjectList->size() == 1) {
        query.AccessControlObject = (*accessControlObjectList)[0];
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TString DoCompress(const TString& data, int quality = 9)
{
    TString compressed;
    TStringOutput output(compressed);
    TZstdCompress compressStream(&output, quality);
    compressStream.Write(data.data(), data.size());
    compressStream.Finish();
    output.Finish();
    return compressed;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

const TString DefaultCompressedValue = DoCompress(TString("{}"));

TString Compress(const TString& data, std::optional<ui64> maxCompressedStringSize, int quality)
{
    auto compressedValue = DoCompress(data, quality);
    return maxCompressedStringSize.has_value() && compressedValue.size() > maxCompressedStringSize.value() ? DefaultCompressedValue : compressedValue;
}

TString Decompress(const std::string& data)
{
    TMemoryInput input(data.begin(), data.size());
    TZstdDecompress decompressStream(&input);
    auto res = decompressStream.ReadAll();
    return res;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
