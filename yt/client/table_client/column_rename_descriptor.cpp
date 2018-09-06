#include "column_rename_descriptor.h"

#include <yt/client/table_client/proto/chunk_meta.pb.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/parser.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/serialize.h>

namespace NYT {
namespace NTableClient {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TColumnRenameDescriptors& value, INodePtr node)
{
    auto mapNode = node->AsMap();
    value.clear();
    for (const auto& pair : mapNode->GetChildren()) {
        value.push_back(TColumnRenameDescriptor());
        value.back().OriginalName = pair.first;
        Deserialize(value.back().NewName, pair.second);
    }
}

void Serialize(const TColumnRenameDescriptors& value, IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    for (const auto& descriptor : value) {
        consumer->OnKeyedItem(descriptor.OriginalName);
        consumer->OnStringScalar(descriptor.NewName);
    }
    consumer->OnEndMap();
}

void ToProto(NProto::TColumnRenameDescriptor* protoDescriptor, const TColumnRenameDescriptor& descriptor)
{
    protoDescriptor->set_original_name(descriptor.OriginalName);
    protoDescriptor->set_new_name(descriptor.NewName);
}

void FromProto(TColumnRenameDescriptor* descriptor, const NProto::TColumnRenameDescriptor& protoDescriptor)
{
    descriptor->OriginalName = protoDescriptor.original_name();
    descriptor->NewName = protoDescriptor.new_name();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
