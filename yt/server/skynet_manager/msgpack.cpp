#include "msgpack.h"

#include <yt/core/yson/consumer.h>
#include <yt/core/ytree/node.h>
#include <yt/core/ytree/tree_builder.h>
#include <yt/core/ytree/ephemeral_node_factory.h>

#include <yt/core/misc/error.h>

#define MSGPACK_NO_BOOST_ADAPTOR
#include <contrib/libs/msgpack/include/msgpack/unpack.hpp>
#include <contrib/libs/msgpack/include/msgpack/pack.hpp>
#include <contrib/libs/msgpack/include/msgpack/sbuffer.hpp>

namespace NYT {
namespace NSkynetManager {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void ConvertToConsumer(const msgpack::object& object, IYsonConsumer* consumer)
{
    switch(object.type) {
    case msgpack::type::NIL:
        consumer->OnEntity();
        return;
    case msgpack::type::BOOLEAN:
        consumer->OnBooleanScalar(object.via.boolean);
        return;
    case msgpack::type::POSITIVE_INTEGER:
        consumer->OnUint64Scalar(object.via.u64);
        return;
    case msgpack::type::NEGATIVE_INTEGER:
        consumer->OnInt64Scalar(object.via.i64);
        return;
    case msgpack::type::FLOAT:
        consumer->OnDoubleScalar(object.via.f64);
        return;
    case msgpack::type::STR:
        consumer->OnStringScalar(TStringBuf(object.via.str.ptr,  object.via.str.size));
        return;
    case msgpack::type::BIN:
        consumer->OnStringScalar(TStringBuf(object.via.bin.ptr,  object.via.bin.size));
        return;
    case msgpack::type::ARRAY: {
        consumer->OnBeginList();
        for (size_t i = 0; i < object.via.array.size; i++) {
            auto item = object.via.array.ptr + i;
            consumer->OnListItem();
            ConvertToConsumer(*item, consumer);
        }
        consumer->OnEndList();
        return;
    }
    case msgpack::type::MAP: {
        consumer->OnBeginMap();
        for (size_t i = 0; i < object.via.map.size; i++) {
            auto item = object.via.map.ptr + i;

            if (item->key.type == msgpack::type::STR) {
                consumer->OnKeyedItem(TStringBuf(item->key.via.str.ptr, item->key.via.str.size));
            } else if (item->key.type == msgpack::type::BIN) {
                consumer->OnKeyedItem(TStringBuf(item->key.via.bin.ptr, item->key.via.bin.size));
            } else {
                THROW_ERROR_EXCEPTION("Unsupported key type in msgpack");
            }

            ConvertToConsumer(item->val, consumer);
        }
        consumer->OnEndMap();
        return;
    }
    default:
        THROW_ERROR_EXCEPTION("Unsupported type in msgpack");
    }
}

void ParseFromMsgpack(const TSharedRef& buffer, IYsonConsumer* consumer)
{
    msgpack::unpacker unpacker;
    unpacker.reserve_buffer(buffer.Size());
    std::copy(buffer.Begin(), buffer.End(), unpacker.buffer());
    unpacker.buffer_consumed(buffer.Size());

    msgpack::object_handle handle;
    if (!unpacker.next(handle)) {
        THROW_ERROR_EXCEPTION("Error parsing msgpack");
    }

    ConvertToConsumer(handle.get(), consumer);

    if (unpacker.next(handle)) {
        THROW_ERROR_EXCEPTION("Found multiple msgpack objects in a single blob");
    }
}

INodePtr ParseFromMsgpack(const TSharedRef& buffer)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    ParseFromMsgpack(buffer, builder.get());
    return builder->EndTree();
}

void WriteToPacker(msgpack::packer<msgpack::sbuffer>* packer, const INodePtr& node)
{
    auto packString = [&] (auto value) {
        // str8 wire type crashes skynet parser. Forcing str32 type.
        char buf[5];
        buf[0] = static_cast<char>(0xdbu);
        _msgpack_store32(&buf[1], static_cast<uint32_t>(value.size()));
        packer->pack_str_body(buf, 5);
        
        packer->pack_str_body(value.data(), value.size());
    };

    switch(node->GetType()) {
    case ENodeType::Entity:
        packer->pack_nil();
        break;
    case ENodeType::Boolean: {
        if (node->GetValue<bool>()) {
            packer->pack_true();
        } else {
            packer->pack_false();
        }
        break;
    }
    case ENodeType::Int64:
        packer->pack_int64(node->GetValue<i64>());
        break;
    case ENodeType::Uint64:
        packer->pack_uint64(node->GetValue<ui64>());
        break;
    case ENodeType::Double:
        packer->pack_double(node->GetValue<double>());
        break;    
    case ENodeType::String: {
        auto value = node->GetValue<TString>();
        packString(value);
        break;
    }
    case ENodeType::List: {
        auto list = node->AsList()->GetChildren();
        packer->pack_array(list.size());
        for (auto&& item : list) {
            WriteToPacker(packer, item);
        }
        break;
    }
    case ENodeType::Map: {
        auto map = node->AsMap()->GetChildren();
        packer->pack_map(map.size());
        for (auto&& item : map) {
            packString(item.first);
            WriteToPacker(packer, item.second);
        }
        break;
    }
    default:
        THROW_ERROR_EXCEPTION("Unsupported node type");
    }
}

struct TMsgpackBufferTag {};

TSharedRef SerializeToMsgpack(const INodePtr& node)
{
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);

    WriteToPacker(&packer, node);

    return TSharedRef::MakeCopy<TMsgpackBufferTag>({buffer.data(), buffer.size()});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
