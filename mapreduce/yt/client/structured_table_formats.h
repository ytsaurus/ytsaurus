#pragma once

#include <mapreduce/yt/interface/fwd.h>

#include <mapreduce/yt/interface/operation.h>
#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/http/requests.h>

#include <utility>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

enum class EIODirection
{
    Input,
    Output,
};

////////////////////////////////////////////////////////////////////////////////

struct TSmallJobFile
{
    TString FileName;
    TString Data;
};

////////////////////////////////////////////////////////////////////////////////

// Table that is used while preparing operation formats. Can be real table or intermediate
struct TStructuredJobTable
{
    TTableStructure Description;
    // Might be null for intermediate tables in MapReduce operation
    TMaybe<TRichYPath> RichYPath;

    static TStructuredJobTable Intermediate(TTableStructure description)
    {
        return TStructuredJobTable{std::move(description), Nothing()};
    }
};
using TStructuredJobTableList = TVector<TStructuredJobTable>;
TString JobTablePathString(const TStructuredJobTable& jobTable);
TStructuredJobTableList ToStructuredJobTableList(const TVector<TStructuredTablePath>& tableList);

TStructuredJobTableList CanonizeStructuredTableList(const TAuth& auth, const TVector<TStructuredTablePath>& tableList);
TVector<TRichYPath> GetPathList(
    const TStructuredJobTableList& tableList,
    const TMaybe<TSchemaInferenceResult>& schemaInferenceResult,
    bool inferSchema);

////////////////////////////////////////////////////////////////////////////////

class TFormatBuilder
{
private:
    struct TFormatSwitcher;

public:
    TFormatBuilder(
        IClientRetryPolicyPtr clientRetryPolicy,
        TAuth auth,
        TTransactionId transactionId,
        TOperationOptions operationOptions);

    std::pair<TFormat, TMaybe<TSmallJobFile>> CreateFormat(
        const IStructuredJob& job,
        const EIODirection& direction,
        const TStructuredJobTableList& structuredTableList,
        const TMaybe<TFormatHints>& formatHints,
        ENodeReaderFormat nodeReaderFormat,
        bool allowFormatFromTableAttribute);

    std::pair<TFormat, TMaybe<TSmallJobFile>> CreateYamrFormat(
        const IStructuredJob& job,
        const EIODirection& direction,
        const TStructuredJobTableList& structuredTableList,
        const TMaybe<TFormatHints>& formatHints,
        ENodeReaderFormat nodeReaderFormat,
        bool allowFormatFromTableAttribute);

    std::pair<TFormat, TMaybe<TSmallJobFile>> CreateNodeFormat(
        const IStructuredJob& job,
        const EIODirection& direction,
        const TStructuredJobTableList& structuredTableList,
        const TMaybe<TFormatHints>& formatHints,
        ENodeReaderFormat nodeReaderFormat,
        bool allowFormatFromTableAttribute);

    std::pair<TFormat, TMaybe<TSmallJobFile>> CreateNodeYdlFormat(
        const IStructuredJob& job,
        const EIODirection& direction,
        const TStructuredJobTableList& structuredTableList,
        const TMaybe<TFormatHints>& formatHints,
        ENodeReaderFormat nodeReaderFormat,
        bool allowFormatFromTableAttribute);

    std::pair<TFormat, TMaybe<TSmallJobFile>> CreateProtobufFormat(
        const IStructuredJob& job,
        const EIODirection& direction,
        const TStructuredJobTableList& structuredTableList,
        const TMaybe<TFormatHints>& formatHints,
        ENodeReaderFormat nodeReaderFormat,
        bool allowFormatFromTableAttribute);

private:
    const IClientRetryPolicyPtr ClientRetryPolicy_;
    const TAuth Auth_;
    const TTransactionId TransactionId_;
    const TOperationOptions OperationOptions_;
};

////////////////////////////////////////////////////////////////////////////////

TMaybe<TTableSchema> GetTableSchema(const TTableStructure& tableStructure);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
