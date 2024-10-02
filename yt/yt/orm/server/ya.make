LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/ya_check_dependencies.inc)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    api/config.cpp
    api/discovery_service.cpp
    api/object_service.cpp
    api/profiling.cpp

    access_control/access_control_hierarchy.cpp
    access_control/access_control_manager.cpp
    access_control/config.cpp
    access_control/data_model_interop.cpp
    access_control/helpers.cpp
    access_control/object_cluster.cpp
    access_control/proto/continuation_token.proto
    access_control/public.cpp
    access_control/request_tracker.cpp
    access_control/subject_cluster.cpp

    master/bootstrap_detail.cpp
    master/config.cpp
    master/db_version_getter.cpp
    master/event_log.cpp
    master/helpers.cpp
    master/service_detail.cpp
    master/yt_connector.cpp

    objects/aggregate_query_executor.cpp
    objects/attribute_values_consumer.cpp
    objects/attribute_matcher.cpp
    objects/attribute_policy.cpp
    objects/attribute_policy_detail.cpp
    objects/attribute_profiler.cpp
    objects/attribute_schema.cpp
    objects/attribute_schema_builder.cpp
    objects/attribute_schema_traits.cpp
    objects/batch_size_backoff.cpp
    objects/cast.cpp
    objects/config.cpp
    objects/connection_validators.cpp
    objects/continuation.cpp
    objects/data_model_object.cpp
    objects/db_schema.cpp
    objects/fqid.cpp
    objects/fetchers.cpp
    objects/get_query_executor.cpp
    objects/helpers.cpp
    objects/history_events.cpp
    objects/history_query.cpp
    objects/history_manager.cpp
    objects/key_storage.cpp
    objects/key_util.cpp
    objects/object.cpp
    objects/object_log.cpp
    objects/object_manager.cpp
    objects/object_reflection.cpp
    objects/object_table_reader.cpp
    objects/order.cpp
    objects/ordered_tablet_reader.cpp
    objects/persistence.cpp
    objects/pool_weights_manager.cpp
    objects/proto_values_consumer.cpp
    objects/proto/continuation_token.proto
    objects/proto/history_event_etc.proto
    objects/proto/objects.proto
    objects/proto/watch_record.proto
    objects/public.cpp
    objects/query_executor_helpers.cpp
    objects/reference_attribute.cpp
    objects/request_handler.cpp
    objects/scalar_attribute_traits.cpp
    objects/select_continuation.cpp
    objects/select_object_history_executor.cpp
    objects/select_query_executor.cpp
    objects/serialize.cpp
    objects/session.cpp
    objects/tags.cpp
    objects/build_tags.cpp
    objects/transaction.cpp
    objects/transaction_manager.cpp
    objects/transaction_wrapper.cpp
    objects/type_handler.cpp
    objects/type_handler_detail.cpp
    objects/watch_log.cpp
    objects/watch_log_consumer_interop.cpp
    objects/watch_log_event_matcher.cpp
    objects/watch_manager.cpp
    objects/watch_query_executor.cpp
)

PEERDIR(
    yt/yt/orm/client/objects

    yt/yt/orm/library/query
    yt/yt/orm/library/attributes

    yt/yt_proto/yt/orm/client/proto
    yt/yt_proto/yt/orm/data_model

    yt/yt/server/lib/rpc_proxy

    yt/yt/ytlib

    yt/yt/client
    yt/yt/client/federated

    yt/yt/library/profiling
    yt/yt/library/profiling/solomon

    yt/yt/library/query/base
    yt/yt/library/query/engine_api

    yt/yt/core

    library/cpp/protobuf/interop
    library/cpp/string_utils/quote
    library/cpp/yson/node
    library/cpp/yt/misc
    library/cpp/iterator
)

END()

RECURSE_FOR_TESTS(
    objects/unittests
)
