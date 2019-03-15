#!/usr/bin/env python

import argparse

import google.protobuf.descriptor_pb2 as protobuf_descriptor_pb2
import yp_proto.yp.client.api.proto.data_model_pb2 as data_model_pb2


def get_modules():
    import yp_proto.yp.client.api.proto.replica_set_pb2 as replica_set_pb2
    import yp_proto.yp.client.api.proto.resource_cache_pb2 as resource_cache_pb2
    import yp_proto.yp.client.api.proto.multi_cluster_replica_set_pb2 as multi_cluster_replica_set_pb2
    import yp_proto.yp.client.api.proto.dynamic_resource_pb2 as dynamic_resource_pb2

    return [
        data_model_pb2,
        replica_set_pb2,
        multi_cluster_replica_set_pb2,
        resource_cache_pb2,
        dynamic_resource_pb2,
    ]


def print_field(field):
    if field.label == protobuf_descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED:
        raise Exception("Required fields are not supported")
    elif field.label == protobuf_descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL:
        label = ""
    elif field.label == protobuf_descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED:
        label = "repeated "
    else:
        raise Exception("Unknown label {}".format(field.label))

    if field.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE or \
       field.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_ENUM:
        field_type = field.type_name[1:]
    elif field.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_STRING:
        field_type = "string"
    elif field.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_UINT32:
        field_type = "uint32"
    elif field.type == protobuf_descriptor_pb2.FieldDescriptorProto.TYPE_UINT64:
        field_type = "uint64"
    else:
        raise Exception("Unknown type {}".format(field.type))

    print "    {}{} {} = {};".format(label, field_type, field.name, field.number)


def print_imports():
    for module in get_modules():
        PREFIX = "yp_proto/"
        name = module.DESCRIPTOR.name
        if not name.startswith(PREFIX):
            raise Exception("Module name {} does not start with {}".format(name, PREFIX))
        print "import \"{}\";".format(name[len(PREFIX):])


def print_separator():
    print "////////////////////////////////////////////////////////////////////////////////"


def generate_client():
    options = []
    meta_base_dict = dict()
    for module in get_modules():
        options += module.DESCRIPTOR.GetOptions().Extensions[data_model_pb2.object_type]
        descriptor = protobuf_descriptor_pb2.FileDescriptorProto.FromString(module.DESCRIPTOR.serialized_pb)
        for message_type in descriptor.message_type:
            T = "T"
            META_BASE = "MetaBase"
            name = message_type.name
            if name.startswith(T) and name.endswith(META_BASE):
                meta_base_dict[name[len(T):-len(META_BASE)]] = message_type

    print """\
// AUTOMATICALLY GENERATED, DO NOT EDIT!

syntax = "proto3";

package NYP.NClient.NApi.NProto;

option java_package = "ru.yandex.yp.client.api";
option java_outer_classname = "Autogen";

import "yp/client/api/proto/enums.proto";
import "yt/core/ytree/proto/attributes.proto";
import "yt/core/yson/proto/protobuf_interop.proto";
"""
    print_imports()
    print ""
    print_separator()
    print ""
    print "enum EObjectType"
    print "{"
    for option in options:
        print "    OT_{} = {}".format(option.snake_case_name.upper(), option.type_value)
        print "    [(NYT.NYson.NProto.enum_value_name) = \"{}\"];".format(option.snake_case_name)
        print ""
    print "    OT_NULL = -1"
    print "    [(NYT.NYson.NProto.enum_value_name) = \"null\"];"
    print "}"
    print ""
    print_separator()
    for option in options:
        print ""
        print "message T{}Meta".format(option.camel_case_name)
        print "{"
        print "    string id = 1;"
        print "    string uuid = 12;"
        print "    string name = 13;"
        print "    EObjectType type = 2;"
        print "    uint64 creation_time = 3;"
        print "    bool inherit_acl = 10;"
        print "    repeated TAccessControlEntry acl = 11;"
        if option.camel_case_name in meta_base_dict:
            print "    // Custom fields:"
            meta_base_type = meta_base_dict[option.camel_case_name]
            for field in meta_base_type.field:
                print_field(field)

        print "}"
        print ""
        print "message T{}".format(option.camel_case_name)
        print "{"
        print "    T{}Meta meta = 1;".format(option.camel_case_name)
        print "    T{}Spec spec = 2;".format(option.camel_case_name)
        print "    T{}Status status = 3;".format(option.camel_case_name)
        print "    NYT.NYTree.NProto.TAttributeDictionary labels = 4;"
        print "    NYT.NYTree.NProto.TAttributeDictionary annotations = 5;"
        print "}"
        print ""
        print "////////////////////////////////////////////////////////////////////////////////"


def generate_server():
    print """\
// AUTOMATICALLY GENERATED, DO NOT EDIT!

syntax = "proto3";

package NYP.NServer.NObjects.NProto;

import "yp/client/api/proto/enums.proto";
"""
    print_imports()
    print ""
    print_separator()
    for module in get_modules():
        descriptor = protobuf_descriptor_pb2.FileDescriptorProto.FromString(module.DESCRIPTOR.serialized_pb)
        for message_type in descriptor.message_type:
            other_fields = list([field for field in message_type.field if field.options.Extensions[data_model_pb2.other] == [True]])
            if len(other_fields) == 0:
                continue

            print ""
            print "message {}Other".format(message_type.name)
            print "{"
            for field in other_fields:
                print_field(field)
            print "}"
            print ""
            print_separator()


def main(arguments):
    if arguments.client:
        generate_client()
    elif arguments.server:
        generate_server()
    else:
        raise Exception("Must specify target to generate")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Generates auxiliary .proto-files for both YP server and client sides")
    parser.add_argument(
        "--client",
        action="store_true",
        default=False,
        help="Generate client-side types",
    )
    parser.add_argument(
        "--server",
        action="store_true",
        default=False,
        help="Generate server-side types",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_arguments())
