#include "structured_table_formats.h"

#include "format_hints.h"
#include "skiff.h"

#include <mapreduce/yt/io/yamr_table_reader.h>

#include <mapreduce/yt/library/table_schema/protobuf.h>

#include <mapreduce/yt/interface/common.h>

#include <mapreduce/yt/raw_client/raw_requests.h>

#include <library/cpp/type_info/type_info.h>
#include <library/cpp/yson/writer.h>

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

TString CreateYdlConfig(const TVector<NTi::TTypePtr>& descriptorList)
{
    TStringStream structHashList;
    for (const auto& descriptor : descriptorList) {
        structHashList << descriptor->GetHash() << Endl;
    }
    return structHashList.Str();
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
        } else if constexpr (std::is_same_v<T, TYdlTableStructure>) {
            TString name;
            if (description.Type->AsStruct()->GetName().Defined()) {
                name = *description.Type->AsStruct()->GetName();
            } else {
                name = "<unknown>";
            }
            return name + " YDL type";
        } else {
            static_assert(TDependentFalse<T>, "Unknown type");
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
    const auto canonized = NRawClient::CanonizeYPaths(/* retryPolicy */ nullptr, auth, toCanonize);
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
    const TMaybe<TVector<TTableSchema>>& jobSchemaInferenceResult,
    bool inferSchemaFromDescriptions)
{
    Y_VERIFY(!jobSchemaInferenceResult || tableList.size() == jobSchemaInferenceResult->size());

    auto maybeInferSchema = [&] (const TStructuredJobTable& table, ui32 tableIndex) -> TMaybe<TTableSchema> {
        if (jobSchemaInferenceResult && !jobSchemaInferenceResult->at(tableIndex).Empty()) {
            return jobSchemaInferenceResult->at(tableIndex);
        }
        if (HoldsAlternative<TYdlTableStructure>(table.Description)) {
            return CreateTableSchema(Get<TYdlTableStructure>(table.Description).Type);
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
        } else if constexpr (std::is_same_v<T, TYdlStructuredRowStream>) {
            return &TFormatBuilder::CreateNodeYdlFormat;
        } else if constexpr (std::is_same_v<T, TVoidStructuredRowStream>) {
            return &TFormatBuilder::CreateVoidFormat;
        } else {
            static_assert(TDependentFalse<T>, "unknown stream description");
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

std::pair<TFormat, TMaybe<TSmallJobFile>> TFormatBuilder::CreateVoidFormat(
    const IStructuredJob& /*job*/,
    const EIODirection& /*direction*/,
    const TStructuredJobTableList& /*structuredTableList*/,
    const TMaybe<TFormatHints>& /*formatHints*/,
    ENodeReaderFormat /*nodeReaderFormat*/,
    bool /*allowFormatFromTableAttribute*/)
{
    return {
        TFormat(),
        Nothing()
    };
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

[[noreturn]] static void ThrowUnsupportedStructureDescription(
    const EIODirection& direction,
    const TStructuredJobTable& table,
    const IStructuredJob& job)
{
    ythrow TApiUsageError()
        << "cannot use " << direction << " table '" << JobTablePathString(table)
        << "' with job " << TJobFactory::Get()->GetJobName(&job) << "; "
        << "table has unsupported structure description; check " << GetAddIOMethodName(direction) << " for this table";
}

[[noreturn]] static void ThrowTypeDeriveFail(
    const EIODirection& direction,
    const IStructuredJob& job,
    const TString& type)
{
    ythrow TApiUsageError()
        << "Cannot derive exact " << type <<  " type for intermediate " << direction << " table for job "
        << TJobFactory::Get()->GetJobName(&job)
        << "; use one of TMapReduceOperationSpec::Hint* methods to specifiy intermediate table structure";
}

[[noreturn]] static void ThrowUnexpectedDifferentDescriptors(
    const EIODirection& direction,
    const TStructuredJobTable& table,
    const IStructuredJob& job,
    const TMaybe<TStringBuf> jobDescriptorName,
    const TMaybe<TStringBuf> descriptorName)
{
    ythrow TApiUsageError()
        << "Job " << TJobFactory::Get()->GetJobName(&job) << " expects "
        << jobDescriptorName << " as " << direction << ", but table " << JobTablePathString(table)
        << " is tagged with " << descriptorName;
}

std::pair<TFormat, TMaybe<TSmallJobFile>> TFormatBuilder::CreateNodeYdlFormat(
    const IStructuredJob& job,
    const EIODirection& direction,
    const TStructuredJobTableList& structuredTableList,
    const TMaybe<TFormatHints>& formatHints,
    ENodeReaderFormat /*nodeReaderFormat*/,
    bool /*allowFormatFromTableAttribute*/)
{
    TVector<NTi::TTypePtr> descriptorList;

    NTi::TTypePtr jobDescriptor =
        Get<TYdlStructuredRowStream>(GetJobStreamDescription(job, direction)).Type;
    Y_ENSURE(!structuredTableList.empty(),
             "empty " << direction << " tables for job " << TJobFactory::Get()->GetJobName(&job));

    for (const auto& table : structuredTableList) {
        NTi::TTypePtr descriptor = nullptr;
        if (HoldsAlternative<TYdlTableStructure>(table.Description)) {
            descriptor = Get<TYdlTableStructure>(table.Description).Type;
        } else if (table.RichYPath) {
            ThrowUnsupportedStructureDescription(direction, table, job);
        }
        if (!descriptor) {
            // It must be intermediate table, because there is no proper way
            // to add such table to spec (AddInput requires to specify proper type).
            Y_VERIFY(!table.RichYPath, "Descriptors for all tables except intermediate must be known");
            if (jobDescriptor) {
                descriptor = jobDescriptor;
            } else {
                ThrowTypeDeriveFail(direction, job, "ydl");
            }
        }
        if (jobDescriptor && descriptor->AsStruct()->GetName() != jobDescriptor->AsStruct()->GetName()) {
            ThrowUnexpectedDifferentDescriptors(
                direction,
                table,
                job,
                jobDescriptor->AsStruct()->GetName(),
                descriptor->AsStruct()->GetName());
        }
        descriptorList.push_back(descriptor);
    }
    Y_VERIFY(!descriptorList.empty(), "Descriptors for ydl format are unknown");

    auto format = TFormat::YsonBinary();
    NYT::NDetail::ApplyFormatHints<TNode>(&format, formatHints);
    return {
        format,
        TSmallJobFile{
            TString("ydl") + GetSuffix(direction),
            CreateYdlConfig(descriptorList),
        },
    };
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
            ThrowUnsupportedStructureDescription(direction, table, job);
        }
        if (!descriptor) {
            // It must be intermediate table, because there is no proper way to add such table to spec
            // (AddInput requires to specify proper message).
            Y_VERIFY(!table.RichYPath, "Descriptors for all tables except intermediate must be known");
            if (jobDescriptor) {
                descriptor = jobDescriptor;
            } else {
                ThrowTypeDeriveFail(direction, job, "protobuf");
            }
        }
        if (jobDescriptor && descriptor != jobDescriptor) {
            ThrowUnexpectedDifferentDescriptors(
                direction,
                table,
                job,
                jobDescriptor->full_name(),
                descriptor->full_name());
        }
        descriptorList.push_back(descriptor);
    }
    Y_VERIFY(!descriptorList.empty(), "Messages for proto format are unknown (empty ProtoDescriptors)");
    return {
        TFormat::Protobuf(descriptorList, TConfig::Get()->ProtobufFormatWithDescriptors),
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
        } else if constexpr (std::is_same_v<T, TYdlTableStructure>) {
            return CreateTableSchema(description.Type);
        } else {
            static_assert(TDependentFalse<T>, "unknown type");
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
