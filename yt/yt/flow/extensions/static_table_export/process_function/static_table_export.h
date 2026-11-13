#pragma once

#include "source.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TProcessFunctionStaticTableExportParameters
    : public TStaticTableExportParameters
{
    //! The schema of the exported tables.
    NTableClient::TTableSchemaPtr Schema;
    //! The per-message process function that turns a source row into table rows; without it, the columns of
    //! #Schema are taken from the source row by name.
    std::optional<std::string> ProcessingFunction;
    NYTree::IMapNodePtr ProcessingFunctionParameters;

    REGISTER_YSON_STRUCT(TProcessFunctionStaticTableExportParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProcessFunctionStaticTableExportParameters);

////////////////////////////////////////////////////////////////////////////////

class TProcessFunctionStaticTableExportSinkController
    : public TStaticTableExportSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TProcessFunctionStaticTableExportParameters);

    TProcessFunctionStaticTableExportSinkController(TSinkControllerContextPtr context, TDynamicSinkControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) override;
    void Sync() override;
    void Commit() override;

private:
    const TStaticTableExportSourceInfo SourceInfo_;
    const IStaticTableExportControllerSourcePtr Source_;

    NTableClient::TTableSchemaPtr GetExportSchema() const override;
    NYTree::INodePtr MakeOwner() const override;
    std::optional<int> FindPartition(const TKey& partitionKey, const std::string& identity) const override;
    NYTree::IAttributeDictionaryPtr BuildPublicationAttributes(std::span<const TStaticTableExportRange> ranges) const override;
};

////////////////////////////////////////////////////////////////////////////////

//! Exports a Logbroker topic or a YT queue, parsing every source row with a process function.
class TProcessFunctionStaticTableExportSink
    : public TStaticTableExportSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TProcessFunctionStaticTableExportParameters);
    using TSinkController = TProcessFunctionStaticTableExportSinkController;

    TProcessFunctionStaticTableExportSink(TSinkContextPtr context, TDynamicSinkContextPtr dynamicContext);

private:
    const TStaticTableExportSourceInfo SourceInfo_;
    const IStaticTableExportPartitionSourcePtr Source_;

    TProcessFunctionStaticTableExportSink(TSinkContextPtr context, TDynamicSinkContextPtr dynamicContext, IStaticTableExportPartitionSourcePtr source);

    TStaticTableExportBinding MakeBinding() override;
    TStaticTableExportMessage GetExportMessage(const TOutputMessage& message) const override;
    void ValidateDynamicSpec() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
