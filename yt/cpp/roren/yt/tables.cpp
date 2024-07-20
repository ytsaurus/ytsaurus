#include "tables.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

NYT::TFormat GetFormat(const std::vector<TTableNode*>& tables)
{
    Y_ENSURE(!tables.empty(), "Unexpected number of tables");

    TVector<const ::google::protobuf::Descriptor*> descriptors;
    auto format = tables[0]->GetTableFormat();
    for (const auto& table : tables) {
        Y_ENSURE(table->GetTableFormat() == format, "Format of tables is different");

        if (format == ETableFormat::Proto) {
            descriptors.emplace_back(table->GetProtoDescriptor());
        }
    }

    switch (format) {
        case ETableFormat::TNode:
            return NYT::TFormat::YsonBinary();
        case ETableFormat::Proto:
            return NYT::TFormat::Protobuf(descriptors, true);
        default:
            Y_ABORT("Unsupported table format");
    }
}

NYT::TFormat GetFormat(const THashMap<TOperationConnector, TTableNode*>& tables)
{
    Y_ENSURE(!tables.empty(), "Unexpected number of tables");

    TVector<const ::google::protobuf::Descriptor*> descriptors;
    auto format = tables.begin()->second->GetTableFormat();
    for (const auto& [_, table] : tables) {
        Y_ENSURE(table->GetTableFormat() == format, "Format of tables is different");

        if (format == ETableFormat::Proto) {
            descriptors.emplace_back(table->GetProtoDescriptor());
        }
    }

    switch (format) {
        case ETableFormat::TNode:
            return NYT::TFormat::YsonBinary();
        case ETableFormat::Proto:
            return NYT::TFormat::Protobuf(descriptors, true);
        default:
            Y_ABORT("Unsupported table format");
    }
}

NYT::TFormat GetFormatWithIntermediate(bool useProtoFormat, const std::vector<TTableNode*>& tables)
{
    if (!useProtoFormat) {
        return NYT::TFormat::YsonBinary();
    }

    TVector<const ::google::protobuf::Descriptor*> descriptors;
    descriptors.emplace_back(TKVProto::GetDescriptor());
    for (const auto& table : tables) {
        auto descriptor = table->GetProtoDescriptor();
        Y_ENSURE(descriptor, "Format of tables is different");
        descriptors.emplace_back(descriptor);
    }

    return NYT::TFormat::Protobuf(descriptors, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
