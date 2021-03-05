#include "virtual_value_directory.h"

#include <yt/yt/ytlib/table_client/proto/virtual_value_directory.pb.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TVirtualValueDirectory* protoDirectory, const TVirtualValueDirectoryPtr& directory)
{
    using NYT::ToProto;

    ToProto(protoDirectory->mutable_names(), directory->NameTable->GetNames());
    for (auto row : directory->Rows) {
        protoDirectory->add_rows(SerializeToString(row));
    }

    ToProto(protoDirectory->mutable_schema(), directory->Schema);
}

void FromProto(TVirtualValueDirectoryPtr* directory, const NProto::TVirtualValueDirectory& protoDirectory)
{
    using NYT::FromProto;

    *directory = New<TVirtualValueDirectory>();

    (*directory)->NameTable = TNameTable::FromKeyColumns(FromProto<std::vector<TString>>(protoDirectory.names()));

    TUnversionedRowsBuilder builder;
    builder.ReserveRows(protoDirectory.rows().size());
    for (const auto& protoRow : protoDirectory.rows()) {
        builder.AddProtoRow(protoRow);
    }
    (*directory)->Rows = builder.Build();

    (*directory)->Schema = FromProto<TTableSchemaPtr>(protoDirectory.schema());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
