#include "structured_table_formats.h"

#include "format_hints.h"
#include "skiff.h"

#include <mapreduce/yt/io/yamr_table_reader.h>

#include <mapreduce/yt/library/table_schema/protobuf.h>

#include <library/yson/writer.h>

#include <memory>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

NSkiff::TSkiffSchemaPtr TryCreateSkiffSchema(
    const TAuth& auth,
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& tables,
    const TOperationOptions& options,
    ENodeReaderFormat nodeReaderFormat)
{
    bool hasInputQuery = options.Spec_.Defined() && options.Spec_->IsMap() && options.Spec_->HasKey("input_query");
    if (hasInputQuery) {
        Y_ENSURE_EX(nodeReaderFormat != ENodeReaderFormat::Skiff,
                    TApiUsageError() << "Cannot use Skiff format for operations with 'input_query' in spec");
        return nullptr;
    }
    return CreateSkiffSchemaIfNecessary(
        auth,
        clientRetryPolicy,
        transactionId,
        nodeReaderFormat,
        tables,
        TCreateSkiffSchemaOptions()
            .HasKeySwitch(true)
            .HasRangeIndex(true));
}

TString CreateSkiffConfig(const NSkiff::TSkiffSchemaPtr& schema)
{
    TString result;
    TStringOutput stream(result);
    TYsonWriter writer(&stream);
    Serialize(schema, &writer);
    return result;
}

TString CreateProtoConfig(const TVector<const ::google::protobuf::Descriptor*>& descriptorList)
{
    TString result;
    TStringOutput messageTypeList(result);
    for (const auto& descriptor : descriptorList) {
        messageTypeList << descriptor->full_name() << Endl;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

struct TGetTableStructureDescriptionStringImpl {
    template<typename T>
    TString operator()(const T& description) {
        if constexpr (std::is_same_v<T, TUnspecifiedTableStructure>) {
            return "Unspecified";
        } else if constexpr (std::is_same_v<T, TProtobufTableStructure>) {
            TString res;
            TStringStream out(res);
            if (description.Descriptor) {
                out << description.Descriptor->full_name();
            } else {
                out << "<unknown>";
            }
            out << " protobuf message";
            return res;
        } else {
            static_assert(TDependentFalse<T>::value, "Unknown type");
        }
    }
};

TString GetTableStructureDescriptionString(const TTableStructure& tableStructure)
{
    return ::Visit(TGetTableStructureDescriptionStringImpl(), tableStructure);
}

////////////////////////////////////////////////////////////////////////////////

TString JobTablePathString(const TStructuredJobTable& jobTable)
{
    if (jobTable.RichYPath) {
        return jobTable.RichYPath->Path_;
    } else {
        return "<intermediate-table>";
    }
}

TStructuredJobTableList ToStructuredJobTableList(const TVector<TStructuredTablePath>& tableList)
{
    TStructuredJobTableList result;
    for (const auto& table : tableList) {
        result.push_back(TStructuredJobTable{table.Description, table.RichYPath});
    }
    return result;
}

TStructuredJobTableList CanonizeStructuredTableList(const TAuth& auth, const TVector<TStructuredTablePath>& tableList)
{
    TVector<TRichYPath> toCanonize;
    toCanonize.reserve(tableList.size());
    for (const auto& table : tableList) {
        toCanonize.emplace_back(table.RichYPath);
    }
    const auto canonized = CanonizePaths(auth, toCanonize);
    Y_VERIFY(canonized.size() == tableList.size());

    TStructuredJobTableList result;
    result.reserve(tableList.size());
    for (size_t i = 0; i != canonized.size(); ++i) {
        result.emplace_back(TStructuredJobTable{tableList[i].Description, canonized[i]});
    }
    return result;
}

TVector<TRichYPath> GetPathList(
    const TStructuredJobTableList& tableList,
    const TMaybe<TSchemaInferenceResult>& jobSchemaInferenceResult,
    bool inferSchemaFromDescriptions)
{
    Y_VERIFY(!jobSchemaInferenceResult || tableList.size() == jobSchemaInferenceResult->size());

    auto maybeInferSchema = [&] (const TStructuredJobTable& table, ui32 tableIndex) -> TMaybe<TTableSchema> {
        if (jobSchemaInferenceResult && !jobSchemaInferenceResult->at(tableIndex).Empty()) {
            return jobSchemaInferenceResult->at(tableIndex);
        }
        if (inferSchemaFromDescriptions) {
            return GetTableSchema(table.Description);
        }
        return Nothing();
    };

    TVector<TRichYPath> result;
    result.reserve(tableList.size());
    for (size_t tableIndex = 0; tableIndex != tableList.size(); ++tableIndex) {
        const auto& table = tableList[tableIndex];
        Y_VERIFY(table.RichYPath, "Cannot get path for intermediate table");
        auto richYPath = *table.RichYPath;
        if (!richYPath.Schema_) {
            if (auto schema = maybeInferSchema(table, tableIndex)) {
                richYPath.Schema(std::move(*schema));
            }
        }

        result.emplace_back(std::move(richYPath));
    }
    return result;
}


TStructuredRowStreamDescription GetJobStreamDescription(
    const IStructuredJob& job,
    EIODirection direction)
{
    switch (direction) {
        case EIODirection::Input:
            return job.GetInputRowStreamDescription();
        case EIODirection::Output:
            return job.GetOutputRowStreamDescription();
        default:
            Y_FAIL("unreachable");
    }
}

TString GetSuffix(EIODirection direction)
{
    switch (direction) {
        case EIODirection::Input:
            return "_input";
        case EIODirection::Output:
            return "_output";
    }
    Y_FAIL("unreachable");
}

TString GetAddIOMethodName(EIODirection direction)
{
    switch (direction) {
        case EIODirection::Input:
            return "AddInput<>";
        case EIODirection::Output:
            return "AddOutput<>";
    }
    Y_FAIL("unreachable");
}

////////////////////////////////////////////////////////////////////////////////

struct TFormatBuilder::TFormatSwitcher
{
    template <typename T>
    auto operator() (const T& /*t*/) {
        if constexpr (std::is_same_v<T, TTNodeStructuredRowStream>) {
            return &TFormatBuilder::CreateNodeFormat;
        } else if constexpr (std::is_same_v<T, TTYaMRRowStructuredRowStream>) {
            return &TFormatBuilder::CreateYamrFormat;
        } else if constexpr (std::is_same_v<T, TProtobufStructuredRowStream>) {
            return &TFormatBuilder::CreateProtobufFormat;
        } else {
            static_assert(TDependentFalse<T>::value, "unknown stream description");
        }
    }
};

TFormatBuilder::TFormatBuilder(
    IClientRetryPolicyPtr clientRetryPolicy,
    TAuth auth,
    TTransactionId transactionId,
    TOperationOptions operationOptions)
    : ClientRetryPolicy_(std::move(clientRetryPolicy))
    , Auth_(std::move(auth))
    , TransactionId_(transactionId)
    , OperationOptions_(std::move(operationOptions))
{ }

std::pair <TFormat, TMaybe<TSmallJobFile>> TFormatBuilder::CreateFormat(
    const IStructuredJob& job,
    const EIODirection& direction,
    const TStructuredJobTableList& structuredTableList,
    const TMaybe <TFormatHints>& formatHints,
    ENodeReaderFormat nodeReaderFormat,
    bool allowFormatFromTableAttribute)
{
    auto jobStreamDescription = GetJobStreamDescription(job, direction);
    auto method = ::Visit(TFormatSwitcher(), jobStreamDescription);
    return (this->*method)(
        job,
        direction,
        structuredTableList,
        formatHints,
        nodeReaderFormat,
        allowFormatFromTableAttribute);
}

std::pair<TFormat, TMaybe<TSmallJobFile>> TFormatBuilder::CreateYamrFormat(
    const IStructuredJob& job,
    const EIODirection& direction,
    const TStructuredJobTableList& structuredTableList,
    const TMaybe<TFormatHints>& /*formatHints*/,
    ENodeReaderFormat /*nodeReaderFormat*/,
    bool allowFormatFromTableAttribute)
{
    for (const auto& table: structuredTableList) {
        if (!HoldsAlternative<TUnspecifiedTableStructure>(table.Description)) {
            ythrow TApiUsageError()
                << "cannot use " << direction << " table '" << JobTablePathString(table)
                << "' with job " << TJobFactory::Get()->GetJobName(&job) << "; "
                << "table has unsupported structure description; check " << GetAddIOMethodName(direction) << " for this table";
        }
    }
    TMaybe<TNode> formatFromTableAttributes;
    if (allowFormatFromTableAttribute && OperationOptions_.UseTableFormats_) {
        TVector<TRichYPath> tableList;
        for (const auto& table: structuredTableList) {
            Y_VERIFY(table.RichYPath, "Cannot use format from table for intermediate table");
            tableList.push_back(*table.RichYPath);
        }
        formatFromTableAttributes = GetTableFormats(ClientRetryPolicy_, Auth_, TransactionId_, tableList);
    }
    if (formatFromTableAttributes) {
        return {
            TFormat(*formatFromTableAttributes),
            Nothing()
        };
    } else {
        auto formatNode = TNode("yamr");
        formatNode.Attributes() = TNode()
            ("lenval", true)
            ("has_subkey", true)
            ("enable_table_index", true);
        return {
            TFormat(formatNode),
            Nothing()
        };
    }
}

std::pair<TFormat, TMaybe<TSmallJobFile>> TFormatBuilder::CreateNodeFormat(
    const IStructuredJob& job,
    const EIODirection& direction,
    const TStructuredJobTableList& structuredTableList,
    const TMaybe<TFormatHints>& formatHints,
    ENodeReaderFormat nodeReaderFormat,
    bool /*allowFormatFromTableAttribute*/)
{
    for (const auto& table: structuredTableList) {
        if (!HoldsAlternative<TUnspecifiedTableStructure>(table.Description)) {
            ythrow TApiUsageError()
                << "cannot use " << direction << " table '" << JobTablePathString(table)
                << "' with job " << TJobFactory::Get()->GetJobName(&job) << "; "
                << "table has unsupported structure description; check AddInput<> / AddOutput<> for this table";
        }
    }
    NSkiff::TSkiffSchemaPtr skiffSchema = nullptr;
    if (nodeReaderFormat != ENodeReaderFormat::Yson) {
        TVector<TRichYPath> tableList;
        for (const auto& table: structuredTableList) {
            Y_VERIFY(table.RichYPath, "Cannot use skiff with temporary tables");
            tableList.emplace_back(*table.RichYPath);
        }
        skiffSchema = TryCreateSkiffSchema(
            Auth_,
            ClientRetryPolicy_,
            TransactionId_,
            tableList,
            OperationOptions_,
            nodeReaderFormat);
    }
    if (skiffSchema) {
        auto format = CreateSkiffFormat(skiffSchema);
        NYT::NDetail::ApplyFormatHints<TNode>(&format, formatHints);
        return {
            CreateSkiffFormat(skiffSchema),
            TSmallJobFile{
                TString("skiff") + GetSuffix(direction),
                CreateSkiffConfig(skiffSchema)
            }
        };
    } else {
        auto format = TFormat::YsonBinary();
        NYT::NDetail::ApplyFormatHints<TNode>(&format, formatHints);
        return {
            format,
            Nothing()
        };
    }
}

std::pair<TFormat, TMaybe<TSmallJobFile>> TFormatBuilder::CreateProtobufFormat(
    const IStructuredJob& job,
    const EIODirection& direction,
    const TStructuredJobTableList& structuredTableList,
    const TMaybe<TFormatHints>& /*formatHints*/,
    ENodeReaderFormat /*nodeReaderFormat*/,
    bool /*allowFormatFromTableAttribute*/)
{
    if (TConfig::Get()->UseClientProtobuf) {
        return {
            TFormat::YsonBinary(),
            TSmallJobFile{
                TString("proto") + GetSuffix(direction),
                CreateProtoConfig({}),
            },
        };
    }
    const ::google::protobuf::Descriptor* const jobDescriptor =
        Get<TProtobufStructuredRowStream>(GetJobStreamDescription(job, direction)).Descriptor;
    Y_ENSURE(!structuredTableList.empty(),
             "empty " << direction << " tables for job " << TJobFactory::Get()->GetJobName(&job));

    TVector<const ::google::protobuf::Descriptor*> descriptorList;
    for (const auto& table : structuredTableList) {
        const ::google::protobuf::Descriptor* descriptor = nullptr;
        if (HoldsAlternative<TProtobufTableStructure>(table.Description)) {
            descriptor = Get<TProtobufTableStructure>(table.Description).Descriptor;
        } else if (table.RichYPath) {
            ythrow TApiUsageError()
                << "cannot use " << direction << " table '" << JobTablePathString(table)
                << "' with job " << TJobFactory::Get()->GetJobName(&job) << "; "
                << "table has unsupported structure description; check " << GetAddIOMethodName(direction) << " for this table";
        }
        if (!descriptor) {
            // It must be intermediate table, because there is no proper way to add such table to spec (AddInput requires to specify proper message).
            Y_VERIFY(!table.RichYPath, "Descriptors for all tables except intermediate must be known");
            if (jobDescriptor) {
                descriptor = jobDescriptor;
            } else {
                ythrow TApiUsageError()
                    << "Cannot derive exact protobuf type for intermediate " << direction << " table for job "
                    << TJobFactory::Get()->GetJobName(&job)
                    << "; use one of TMapreduceOperationSpec::Hint* methods to specifiy intermediate table structure";
            }
        }
        if (jobDescriptor && descriptor != jobDescriptor) {
            ythrow TApiUsageError()
                << "Job " << TJobFactory::Get()->GetJobName(&job) << " expects "
                << jobDescriptor->full_name() << " as " << direction << ", but table " << JobTablePathString(table)
                << " is tagged with " << descriptor->full_name();
        }
        descriptorList.push_back(descriptor);
    }
    Y_VERIFY(!descriptorList.empty(), "Messages for proto format are unknown (empty ProtoDescriptors)");
    return {
        TFormat::Protobuf(descriptorList),
        TSmallJobFile{
            TString("proto") + GetSuffix(direction),
            CreateProtoConfig(descriptorList)
        },
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TGetTableSchemaImpl
{
    template <typename T>
    TMaybe<TTableSchema> operator() (const T& description) {
        if constexpr (std::is_same_v<T, TUnspecifiedTableStructure>) {
            return Nothing();
        } else if constexpr (std::is_same_v<T, TProtobufTableStructure>) {
            if (!description.Descriptor) {
                return Nothing();
            }
            return CreateTableSchema(*description.Descriptor);
        } else {
            static_assert(TDependentFalse<T>::value, "unknown type");
        }
    }
};

TMaybe<TTableSchema> GetTableSchema(const TTableStructure& tableStructure)
{
    return Visit(TGetTableSchemaImpl(), tableStructure);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
