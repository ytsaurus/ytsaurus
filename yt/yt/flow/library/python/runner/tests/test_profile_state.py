import copy

import pytest
from google.protobuf.api_pb2 import Api
from google.protobuf.descriptor_pb2 import FileDescriptorProto, FileDescriptorSet
from google.protobuf.descriptor_pool import DescriptorPool
from yt.wrapper import yson

from yt.yt.flow.library.python import runner
from yt.yt.flow.library.python.runner import profile_state

KINDS = [
    ("external_state_managers", "external_state_manager_class_name", "TProfileStateManager", "proto_external_states"),
    (
        "external_state_joiners",
        "external_state_joiner_class_name",
        "TProfileStateJoiner",
        "joined_proto_external_states",
    ),
]


def _spec(kind, parameters=None, *, class_key=None, class_name=None):
    section, key, cls, _ = kind
    entry = {
        class_key or key: class_name or "NYT::NFlow::NProfileState::" + cls,
        "parameters": {"path": "//state"} if parameters is None else parameters,
    }
    return {"computations": {"join": {section: {"/state": entry}}}}, entry


def _patch(spec, kind, proto_class=Api):
    profile_state.patch_profile_states(spec, **{kind[3]: {"join": {"/state": proto_class}}})


def _load_pool(layout):
    descriptors = FileDescriptorSet.FromString(yson.get_bytes(layout["descriptor_set"]))
    pool = DescriptorPool()
    for descriptor in descriptors.file:
        pool.AddSerializedFile(descriptor.SerializeToString())
    return descriptors, pool


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("alias", [False, True])
def test_descriptor_closure_and_options(kind, alias):
    spec, entry = _spec(
        kind,
        {
            "path": "//state",
            "descriptor_layout": {"name_policy": "snake_case", "codec_column_name": "codec", "default_format_id": 7},
        },
        class_key="class_name" if alias else None,
    )

    _patch(spec, kind)

    layout = entry["parameters"]["descriptor_layout"]
    assert layout["name_policy"] == "snake_case"
    assert layout["codec_column_name"] == "codec"
    assert layout["default_format_id"] == 7
    assert entry["parameters"]["path"] == "//state"
    assert layout["message_name"] == Api.DESCRIPTOR.full_name
    descriptors, pool = _load_pool(layout)
    names = [file.name for file in descriptors.file]
    assert names[-1] == Api.DESCRIPTOR.file.name
    assert "google/protobuf/any.proto" in names
    assert len(names) == len(set(names))
    assert (
        pool.FindMessageTypeByName(Api.DESCRIPTOR.full_name).fields_by_name.keys()
        == Api.DESCRIPTOR.fields_by_name.keys()
    )
    before = yson.dumps(spec)
    _patch(spec, kind)
    assert yson.dumps(spec) == before


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "parameters",
    [
        {"layout": b"\xff\x00"},
        {"descriptor_layout": {"message_name": "other.Message", "descriptor_set": b"\xff\x00"}},
        {"descriptor_layout": {"message_name": "cpp.Message"}},
    ],
)
def test_explicit_source_is_preserved(kind, parameters):
    spec, _ = _spec(kind, parameters)
    # Exercise the same binary string proxies as runner.launch().
    spec = yson.loads(yson.dumps(spec))
    before = yson.dumps(spec)
    _patch(spec, kind)
    assert yson.dumps(spec) == before


@pytest.mark.parametrize("kind", KINDS)
def test_empty_sources_are_filled(kind):
    spec, entry = _spec(
        kind,
        {
            "layout": "",
            "descriptor_layout": {
                "descriptor_set": b"",
                "message_name": Api.DESCRIPTOR.full_name,
            },
        },
    )
    _patch(spec, kind)
    _load_pool(entry["parameters"]["descriptor_layout"])


@pytest.mark.parametrize("kind", KINDS)
def test_undeclared_message_can_use_worker_pool(kind):
    spec, _ = _spec(kind, {"descriptor_layout": {"message_name": "cpp.Message"}})
    before = copy.deepcopy(spec)
    profile_state.patch_profile_states(spec)
    assert spec == before


@pytest.mark.parametrize("kind", KINDS)
def test_missing_source_fails_before_launch(kind):
    spec, _ = _spec(kind)
    with pytest.raises(ValueError, match="Profile state /state of computation join.*No descriptor source"):
        profile_state.patch_profile_states(spec)


@pytest.mark.parametrize("kind", KINDS)
def test_other_implementations_are_untouched(kind):
    spec, _ = _spec(kind, class_name="OtherManager")
    before = copy.deepcopy(spec)
    profile_state.patch_profile_states(spec)
    assert spec == before
    with pytest.raises(ValueError, match="Declared proto state requires"):
        _patch(spec, kind)


def test_manager_and_joiner_declarations_are_scoped_to_computation_and_kind():
    owner, owner_entry = _spec(KINDS[0])
    reader, reader_entry = _spec(KINDS[1])
    spec = {
        "computations": {
            "owner": {**owner["computations"]["join"], **reader["computations"]["join"]},
            "other": copy.deepcopy(owner["computations"]["join"]),
        }
    }
    profile_state.patch_profile_states(
        spec,
        proto_external_states={"owner": {"/state": Api}, "other": {"/state": FileDescriptorProto}},
        joined_proto_external_states={"owner": {"/state": FileDescriptorSet}},
    )
    assert owner_entry["parameters"]["descriptor_layout"]["message_name"] == Api.DESCRIPTOR.full_name
    assert reader_entry["parameters"]["descriptor_layout"]["message_name"] == FileDescriptorSet.DESCRIPTOR.full_name
    other = spec["computations"]["other"]["external_state_managers"]["/state"]
    assert other["parameters"]["descriptor_layout"]["message_name"] == FileDescriptorProto.DESCRIPTOR.full_name


@pytest.mark.parametrize(
    "declarations, error",
    [
        ({"typo": {"/state": Api}}, "unknown computation typo"),
        ({"join": {"/typo": Api}}, "Declared profile state /typo is absent"),
    ],
)
def test_unused_declarations_fail(declarations, error):
    spec, _ = _spec(KINDS[0])
    with pytest.raises(ValueError, match=error):
        profile_state.patch_profile_states(spec, proto_external_states=declarations)


@pytest.mark.parametrize(
    "parameters",
    [
        None,
        {"descriptor_layout": None},
        {"descriptor_layout": {"message_name": 123}},
        {"descriptor_layout": {"descriptor_set": []}},
        {"layout": 1},
    ],
)
def test_malformed_parameters_are_not_replaced(parameters):
    spec, entry = _spec(KINDS[0])
    entry["parameters"] = parameters
    before = copy.deepcopy(spec)
    with pytest.raises(ValueError, match="Profile state /state of computation join"):
        _patch(spec, KINDS[0])
    assert spec == before


def test_size_limit(monkeypatch):
    spec, entry = _spec(KINDS[0])
    size = len(profile_state._serialize_descriptor_set(Api.DESCRIPTOR.file))
    monkeypatch.setattr(profile_state, "_MAX_DESCRIPTOR_SET_SIZE", size - 1)
    with pytest.raises(ValueError, match="over the .* limit of flow_server"):
        _patch(spec, KINDS[0])
    assert "descriptor_layout" not in entry["parameters"]
    monkeypatch.setattr(profile_state, "_MAX_DESCRIPTOR_SET_SIZE", size)
    _patch(spec, KINDS[0])


def _launch(monkeypatch, tmp_path, config, **declarations):
    path = tmp_path / "pipeline.yson"
    original = yson.dumps(config)
    path.write_bytes(original)
    monkeypatch.setattr(runner.tempfile, "tempdir", str(tmp_path))
    calls = []
    monkeypatch.setattr(runner.os, "execv", lambda executable, args: calls.append(args))
    runner.launch(str(path), "/flow_server", **declarations)
    assert len(calls) == 1
    assert path.read_bytes() == original
    with open(calls[0][2], "rb") as source:
        return yson.load(source)


@pytest.mark.parametrize("vanilla", [None, {"enable": False}, {"enable": True}])
def test_launch_always_embeds_descriptors(monkeypatch, tmp_path, vanilla):
    spec, _ = _spec(KINDS[0])
    joiner, _ = _spec(KINDS[1])
    spec["computations"]["join"].update(joiner["computations"]["join"])
    config = {"spec": spec}
    if vanilla is not None:
        config["vanilla"] = vanilla
    generated = _launch(
        monkeypatch,
        tmp_path,
        config,
        proto_external_states={"join": {"/state": Api}},
        joined_proto_external_states={"join": {"/state": Api}},
    )
    for section, *_ in KINDS:
        layout = generated["spec"]["computations"]["join"][section]["/state"]["parameters"]["descriptor_layout"]
        _, pool = _load_pool(layout)
        assert pool.FindMessageTypeByName(Api.DESCRIPTOR.full_name)


def test_launch_preserves_non_utf8_descriptor_bytes(monkeypatch, tmp_path):
    spec, _ = _spec(
        KINDS[0],
        {
            "descriptor_layout": {
                "message_name": "explicit.Message",
                "descriptor_set": b"\x00\xff\xfe",
            }
        },
    )
    generated = _launch(monkeypatch, tmp_path, {"spec": spec}, proto_external_states={"join": {"/state": Api}})
    layout = generated["spec"]["computations"]["join"]["external_state_managers"]["/state"]["parameters"][
        "descriptor_layout"
    ]
    assert yson.get_bytes(layout["descriptor_set"]) == b"\x00\xff\xfe"
