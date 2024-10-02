# !/usr/bin/python3

from yt.yt.orm.codegen.generator import (
    AuxiliaryParameters,
    CodegenParameters,
    dispatch,
)

from yt.yt.orm.example.client.proto import data_model as example_client_api

from yt_proto.yt.orm import data_model as orm_data_model

if __name__ == "__main__":
    aux_parameters = AuxiliaryParameters()

    # Common.
    aux_parameters.default_db_name = "example"
    # Server C++.
    aux_parameters.cpp_server_namespace = "NYT::NOrm::NExample::NServer::NLibrary"
    aux_parameters.cpp_server_path = "yt/yt/orm/example/server/library"
    aux_parameters.cpp_server_plugins_namespace = "NYT::NOrm::NExample::NServer::NPlugins"
    aux_parameters.cpp_server_plugins_path = "yt/yt/orm/example/plugins/server/library"
    aux_parameters.cpp_server_custom_base_type_handler_file_name = "custom_base_type_handler.h"
    aux_parameters.generated_files_dir_name = "autogen"
    # Server proto.
    aux_parameters.server_go_package = "a.yandex-team.ru/yt/yt/orm/example/server/proto/autogen"
    aux_parameters.server_java_package = "ru.yandex.yt.yt.orm.example.server.proto.autogen"
    aux_parameters.server_proto_package = "NYT.NOrm.NExample.NServer.NProto.NAutogen"
    aux_parameters.server_proto_path = "yt/yt/orm/example/server/proto/autogen"
    # Client.
    aux_parameters.client_go_package = "a.yandex-team.ru/yt/yt/orm/example/client/proto/data_model/autogen"
    aux_parameters.client_java_package = "ru.yandex.yt.yt.orm.example.client.proto.data_model.autogen"
    aux_parameters.client_proto_package = "NYT.NOrm.NExample.NClient.NProto"
    aux_parameters.client_misc_cpp_namespace = "NYT::NOrm::NExample::NClient::NApi"
    aux_parameters.client_misc_lib = "yt/yt/orm/example/client/misc/autogen"
    aux_parameters.client_native_cpp_namespace = "NYT::NOrm::NExample::NClient::NNative"
    aux_parameters.client_native_cpp_path = "yt/yt/orm/example/client/native/autogen"
    aux_parameters.client_objects_cpp_namespace = "NYT::NOrm::NExample::NClient::NObjects"
    aux_parameters.client_objects_cpp_path = "yt/yt/orm/example/client/objects/autogen"
    aux_parameters.data_model_proto_package = "NYT.NOrm.NExample.NClient.NProto.NDataModel"
    aux_parameters.data_model_proto_path = "yt/yt/orm/example/client/proto/data_model"
    # DB.
    aux_parameters.db_version = 2

    codegen_parameters = CodegenParameters(
        source_packages=[example_client_api],
        inplace_packages=[orm_data_model],
        aux_parameters=aux_parameters,
    )

    dispatch(obj=codegen_parameters)
