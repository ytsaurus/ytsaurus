#include <yt/core/test_framework/framework.h>

#include <yp/server/objects/stage.h>
#include <yp/server/objects/stage_type_handler.h>

namespace NYP::NServer::NObjects::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

NInfra::NPodAgent::API::TEnvVar LiteralEnvVar(const TString& name, const TString& value)
{
    NInfra::NPodAgent::API::TEnvVar result;
    result.set_name(name);
    result.mutable_value()->mutable_literal_env()->set_value(value);
    return result;
}

void AddStaticResourceToPodAgentSpec(
    NInfra::NPodAgent::API::TPodAgentSpec& spec,
    const TString& id)
{
    NInfra::NPodAgent::API::TResource* staticResource = spec.mutable_resources()->add_static_resources();
    staticResource->set_id(id);
}

void AddLayerToPodAgentSpec(
    NInfra::NPodAgent::API::TPodAgentSpec& spec,
    const TString& id)
{
    NInfra::NPodAgent::API::TLayer* layer = spec.mutable_resources()->add_layers();
    layer->set_id(id);
}

void AddVolumeToPodAgentSpec(
    NInfra::NPodAgent::API::TPodAgentSpec& spec,
    const TString& id,
    const std::vector<TString>& layerRefs)
{
    NInfra::NPodAgent::API::TVolume* volume = spec.add_volumes();
    volume->set_id(id);
    for (const TString& layerRef : layerRefs) {
        volume->mutable_generic()->add_layer_refs(layerRef);
    }
}

void AddBoxToPodAgentSpec(
    NInfra::NPodAgent::API::TPodAgentSpec& spec,
    const TString& id,
    const std::vector<TString>& staticResourceRefs,
    const std::vector<TString>& rootfsLayerRefs,
    const std::vector<TString>& volumesRefs)
{
    NInfra::NPodAgent::API::TBox* box = spec.add_boxes();
    box->set_id(id);
    for (const TString& staticResourceRef : staticResourceRefs) {
        box->add_static_resources()->set_resource_ref(staticResourceRef);
    }
    for (const TString& rootfsLayerRef : rootfsLayerRefs) {
        box->mutable_rootfs()->add_layer_refs(rootfsLayerRef);
    }
    for (const TString& volumesRef : volumesRefs) {
        box->add_volumes()->set_volume_ref(volumesRef);
    }
}

void AddWorkloadToPodAgentSpec(
    NInfra::NPodAgent::API::TPodAgentSpec& spec,
    const TString& id,
    const TString& boxRef)
{
    NInfra::NPodAgent::API::TWorkload* workload = spec.add_workloads();
    workload->set_id(id);
    workload->set_box_ref(boxRef);
}

void AddMutableWorkloadToPodAgentSpec(
    NInfra::NPodAgent::API::TPodAgentSpec& spec,
    const TString& workloadRef)
{
    NInfra::NPodAgent::API::TMutableWorkload* mutableWorkload = spec.add_mutable_workloads();
    mutableWorkload->set_workload_ref(workloadRef);
}

TEST(StageValidator, ValidateStageDeployUnitIdTest)
{
    ASSERT_THROW(ValidateId("inv*", "Stage id"), TErrorException);

    ValidateId("valid", "Stage id");
}

TEST(StageValidator, ValidateStageIdLengthTest)
{
    TObjectId id = TString("a") * 75;
    ASSERT_THROW(ValidateId(id, "Stage id"), TErrorException);
}

TEST(StageValidator, ValidateTvmConfigTest)
{
    NClient::NApi::NProto::TTvmConfig config;
    config.set_mode(NClient::NApi::NProto::TTvmConfig_EMode_ENABLED);
    config.add_clients()->mutable_source()->set_alias("alias_source");
    config.add_clients()->mutable_source()->set_alias("alias_source");
    ASSERT_THROW(ValidateTvmConfig(config), TErrorException);

    config.clear_clients();
    config.add_clients()->mutable_source()->set_alias("alias_source");
    config.mutable_clients(0)->mutable_source()->set_app_id(137);
    config.mutable_clients(0)->add_destinations()->set_alias("alias_destination");
    config.mutable_clients(0)->add_destinations()->set_alias("alias_destination");
    ASSERT_THROW(ValidateTvmConfig(config), TErrorException);
}

TEST(StageValidator, ValidatePodAgentObjectIdTest)
{
    ASSERT_THROW(ValidatePodAgentObjectId("box/workload_id", "Workload id"), TErrorException);

    ValidatePodAgentObjectId("workload1_-@:.", "Workload id");
}

TEST(StageValidator, ValidatePodAgentObjectIdLengthTest)
{
    TObjectId id = TString("a") * 75;
    ASSERT_THROW(ValidatePodAgentObjectId(id, "Workload id"), TErrorException);
}

TEST(StageValidator, ValidatePodAgentObjectEnvTest)
{
    google::protobuf::RepeatedPtrField<NInfra::NPodAgent::API::TEnvVar> env;

    *env.Add() = LiteralEnvVar("NAME1", "value1");
    *env.Add() = LiteralEnvVar("NAME2", "value2");
    ValidatePodAgentObjectEnv(env, "workload_id", "workload");

    *env.Add() = LiteralEnvVar("NAME1", "value3");
    ASSERT_THROW(ValidatePodAgentObjectEnv(env, "workload_id", "workload"), TErrorException);
}

TEST(StageValidator, ValidatePodAgentSpecSameIdTest)
{
    static auto checkSameId = [] (std::function<void(NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id)> addObject) {
        NInfra::NPodAgent::API::TPodAgentSpec spec;
        google::protobuf::Map<google::protobuf::string, NClient::NApi::NProto::TDockerImageDescription> emptyMap;

        addObject(spec, "id1");
        addObject(spec, "id2");
        ValidatePodAgentSpec(spec, emptyMap);

        addObject(spec, "id1");
        ASSERT_THROW(ValidatePodAgentSpec(spec, emptyMap), TErrorException);
    };

    // Static resource.
    checkSameId([] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id) {
        AddStaticResourceToPodAgentSpec(spec, id);
    });

    // Layer.
    checkSameId([] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id) {
        AddLayerToPodAgentSpec(spec, id);
    });

    // Volume.
    checkSameId([] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id) {
        AddVolumeToPodAgentSpec(spec, id, {});
    });

    // Box.
    checkSameId([] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id) {
        AddBoxToPodAgentSpec(spec, id, {}, {}, {});
    });

    // Workload.
    checkSameId([] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id) {
        if (spec.boxes().empty()) {
            AddBoxToPodAgentSpec(spec, "box_ref", {}, {}, {});
        }
        AddWorkloadToPodAgentSpec(spec, id, "box_ref");
        AddMutableWorkloadToPodAgentSpec(spec, id);
    });
}

TEST(StageValidator, ValidatePodAgentSpecBadRefsTest)
{
    static auto checkBadRef = [] (
        std::function<void(NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id, const TString& refId)> addObjectWithRef,
        std::function<void(NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& refId)> addRef)
    {
        NInfra::NPodAgent::API::TPodAgentSpec spec;
        google::protobuf::Map<google::protobuf::string, NClient::NApi::NProto::TDockerImageDescription> emptyMap;

        addObjectWithRef(spec, "id", "refId");
        ASSERT_THROW(ValidatePodAgentSpec(spec, emptyMap), TErrorException);

        addRef(spec, "refId");
        ValidatePodAgentSpec(spec, emptyMap);
    };

    // Volume -> layer.
    checkBadRef(
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id, const TString& refId) {
            AddVolumeToPodAgentSpec(spec, id, {refId});
        },
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& refId) {
            AddLayerToPodAgentSpec(spec, refId);
        }
    );

    // Box -> static resource.
    checkBadRef(
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id, const TString& refId) {
            AddBoxToPodAgentSpec(spec, id, {refId}, {}, {});
        },
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& refId) {
            AddStaticResourceToPodAgentSpec(spec, refId);
        }
    );

    // Box -> layer.
    checkBadRef(
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id, const TString& refId) {
            AddBoxToPodAgentSpec(spec, id, {}, {refId}, {});
        },
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& refId) {
            AddLayerToPodAgentSpec(spec, refId);
        }
    );

    // Box -> volume.
    checkBadRef(
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id, const TString& refId) {
            AddBoxToPodAgentSpec(spec, id, {}, {}, {refId});
        },
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& refId) {
            AddVolumeToPodAgentSpec(spec, refId, {});
        }
    );

    // Workload -> box.
    checkBadRef(
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& id, const TString& refId) {
            AddWorkloadToPodAgentSpec(spec, id, refId);
            AddMutableWorkloadToPodAgentSpec(spec, id);
        },
        [] (NInfra::NPodAgent::API::TPodAgentSpec& spec, const TString& refId) {
            AddBoxToPodAgentSpec(spec, refId, {}, {}, {});
        }
    );
}

TEST(StageValidator, ValidatePodAgentSpecWorkloadAndMutableWorkloadTest)
{
    NInfra::NPodAgent::API::TPodAgentSpec spec;
    google::protobuf::Map<google::protobuf::string, NClient::NApi::NProto::TDockerImageDescription> emptyMap;

    AddBoxToPodAgentSpec(spec, "box_ref", {}, {}, {});

    AddWorkloadToPodAgentSpec(spec, "id", "box_ref");
    // Workload, no mutable workload.
    ASSERT_THROW(ValidatePodAgentSpec(spec, emptyMap), TErrorException);

    AddMutableWorkloadToPodAgentSpec(spec, "id");
    ValidatePodAgentSpec(spec, emptyMap);

    spec.clear_workloads();
    // No workload, mutable workload.
    ASSERT_THROW(ValidatePodAgentSpec(spec, emptyMap), TErrorException);
}

TEST(StageValidator, ValidatePodAgentSpecBoxIdForDockerImagesTest)
{
    NInfra::NPodAgent::API::TPodAgentSpec spec;
    google::protobuf::Map<google::protobuf::string, NClient::NApi::NProto::TDockerImageDescription> imagesForBoxes;
    imagesForBoxes.insert({"box", NClient::NApi::NProto::TDockerImageDescription()});

    // No box.
    ASSERT_THROW(ValidatePodAgentSpec(spec, imagesForBoxes), TErrorException);

    AddBoxToPodAgentSpec(spec, "box", {}, {}, {});

    ValidatePodAgentSpec(spec, imagesForBoxes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServer::NObjects::NTests
