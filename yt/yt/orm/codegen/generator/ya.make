PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py

    cli.py
    dynamic_parameters.py
    engine.py
    parameters.py
)

PEERDIR(
    yt/yt/orm/codegen/model

    yt/yt_proto/yt/orm/client/proto
    yt/yt/orm/library/snapshot/codegen

    contrib/python/Jinja2
    contrib/python/click

    library/python/resource
)

RESOURCE(
    templates/aux_data_model.proto.j2 /aux_data_model.proto.j2
    templates/aux_object_types.proto.j2 /aux_object_types.proto.j2
    templates/aux_tags_enum.proto.j2 /aux_tags_enum.proto.j2
    templates/config.cpp.j2 /config.cpp.j2
    templates/config.h.j2 /config.h.j2
    templates/continuation_token.proto.j2 /continuation_token.proto.j2
    templates/access_control.proto.j2 /access_control.proto.j2
    templates/data_model.proto.j2 /data_model.proto.j2
    templates/data_model_objects.proto.j2 /data_model_objects.proto.j2
    templates/db_schema.cpp.j2 /db_schema.cpp.j2
    templates/db_schema.h.j2 /db_schema.h.j2
    templates/db_versions_init.py.j2 /db_versions_init.py.j2
    templates/db_versions_ya.make.j2 /db_versions_ya.make.j2
    templates/discovery_service.proto.j2 /discovery_service.proto.j2
    templates/dynamic_config_manager.cpp.j2 /dynamic_config_manager.cpp.j2
    templates/dynamic_config_manager.h.j2 /dynamic_config_manager.h.j2
    templates/error.h.j2 /error.h.j2
    templates/error.proto.j2 /error.proto.j2
    templates/etc.proto.j2 /etc.proto.j2
    templates/generated_dir_ya.make.inc.j2 /generated_dir_ya.make.inc.j2
    templates/macros.proto.j2 /macros.proto.j2
    templates/macros_object.j2 /macros_object.j2
    templates/main.cpp.j2 /main.cpp.j2
    templates/object_detail.cpp.j2 /object_detail.cpp.j2
    templates/object_detail.h.j2 /object_detail.h.j2
    templates/object_manager.cpp.j2 /object_manager.cpp.j2
    templates/object_manager.h.j2 /object_manager.h.j2
    templates/object_service.proto.j2 /object_service.proto.j2
    templates/object.cpp.j2 /object.cpp.j2
    templates/object.h.j2 /object.h.j2
    templates/objects_aux.h.j2 /objects_aux.h.j2
    templates/program.cpp.j2 /program.cpp.j2
    templates/program.h.j2 /program.h.j2
    templates/public.cpp.j2 /public.cpp.j2
    templates/public.h.j2 /public.h.j2
    templates/type_handlers.h.j2 /type_handlers.h.j2
    templates/type_handler.cpp.j2 /type_handler.cpp.j2
    templates/type_handler.h.j2 /type_handler.h.j2
    templates/yt_schema.py.j2 /yt_schema.py.j2

    templates/client/misc/enums.cpp.j2 /client/misc/enums.cpp.j2
    templates/client/misc/enums.h.j2 /client/misc/enums.h.j2
    templates/client/misc/index_helpers.cpp.j2 /client/misc/index_helpers.cpp.j2
    templates/client/misc/index_helpers.h.j2 /client/misc/index_helpers.h.j2
    templates/client/misc/schema_transitive.h.j2 /client/misc/schema_transitive.h.j2
    templates/client/misc/traits.cpp.j2 /client/misc/traits.cpp.j2
    templates/client/misc/traits.h.j2 /client/misc/traits.h.j2

    templates/client/native/client.cpp.j2 /client/native/client.cpp.j2
    templates/client/native/client.h.j2 /client/native/client.h.j2
    templates/client/native/config.cpp.j2 /client/native/config.cpp.j2
    templates/client/native/config.h.j2 /client/native/config.h.j2
    templates/client/native/discovery_service_proxy.cpp.j2 /client/native/discovery_service_proxy.cpp.j2
    templates/client/native/discovery_service_proxy.h.j2 /client/native/discovery_service_proxy.h.j2
    templates/client/native/object_service_proxy.cpp.j2 /client/native/object_service_proxy.cpp.j2
    templates/client/native/object_service_proxy.h.j2 /client/native/object_service_proxy.h.j2
    templates/client/native/public.cpp.j2 /client/native/public.cpp.j2
    templates/client/native/public.h.j2 /client/native/public.h.j2

    templates/client/objects/init.cpp.j2 /client/objects/init.cpp.j2
    templates/client/objects/init.h.j2 /client/objects/init.h.j2
    templates/client/objects/public.cpp.j2 /client/objects/public.cpp.j2
    templates/client/objects/public.h.j2 /client/objects/public.h.j2
    templates/client/objects/acl.cpp.j2 /client/objects/acl.cpp.j2
    templates/client/objects/acl.h.j2 /client/objects/acl.h.j2
    templates/client/objects/tags.cpp.j2 /client/objects/tags.cpp.j2
    templates/client/objects/tags.h.j2 /client/objects/tags.h.j2
    templates/client/objects/type.cpp.j2 /client/objects/type.cpp.j2
    templates/client/objects/type.h.j2 /client/objects/type.h.j2
)

END()
