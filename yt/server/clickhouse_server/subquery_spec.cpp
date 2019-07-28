#include "subquery_spec.h"

#include "table.h"
#include "table_schema.h"

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <DataTypes/DataTypeFactory.h>

namespace DB {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NYT::NClickHouseServer::NProto::TNameAndTypePair* protoPair, const NameAndTypePair& pair)
{
    protoPair->set_name(TString(pair.name));
    protoPair->set_type(TString(pair.type->getName()));
}

void FromProto(NameAndTypePair* pair, const NYT::NClickHouseServer::NProto::TNameAndTypePair& protoPair)
{
    pair->name = protoPair.name();
    pair->type = DB::DataTypeFactory::instance().get(protoPair.type());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace DB

namespace NYT::NClickHouseServer {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSubquerySpec* protoSpec, const TSubquerySpec& spec)
{
    using NYT::ToProto;

    ToProto(protoSpec->mutable_data_source_directory(), spec.DataSourceDirectory);

    for (const auto& inputDataSliceDescriptors : spec.DataSliceDescriptors) {
        auto* inputSpec = protoSpec->add_input_specs();
        ToProto(
            inputSpec->mutable_chunk_specs(),
            inputSpec->mutable_chunk_spec_count_per_data_slice(),
            inputDataSliceDescriptors);
    }

    if (spec.NodeDirectory) {
        spec.NodeDirectory->DumpTo(protoSpec->mutable_node_directory());
    }

    ToProto(protoSpec->mutable_initial_query_id(), spec.InitialQueryId);
    for (const auto& column : spec.Columns) {
        ToProto(protoSpec->add_columns(), column);
    }
    ToProto(protoSpec->mutable_read_schema(), spec.ReadSchema);

    protoSpec->set_membership_hint(spec.MembershipHint.GetData());
}

void FromProto(TSubquerySpec* spec, const NProto::TSubquerySpec& protoSpec)
{
    using NYT::FromProto;

    FromProto(&spec->DataSourceDirectory, protoSpec.data_source_directory());

    for (const auto& inputSpec : protoSpec.input_specs()) {
        FromProto(
            &spec->DataSliceDescriptors.emplace_back(),
            inputSpec.chunk_specs(),
            inputSpec.chunk_spec_count_per_data_slice());
    }

    if (protoSpec.has_node_directory()) {
        spec->NodeDirectory = New<TNodeDirectory>();
        spec->NodeDirectory->MergeFrom(protoSpec.node_directory());
    }

    FromProto(&spec->InitialQueryId, protoSpec.initial_query_id());
    for (const auto& protoColumn : protoSpec.columns()) {
        FromProto(&spec->Columns.emplace_back(), protoColumn);
    }
    FromProto(&spec->ReadSchema, protoSpec.read_schema());

    spec->MembershipHint = TYsonString(protoSpec.membership_hint());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
