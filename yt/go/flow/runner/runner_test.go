package runner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestParseArgsAcceptsSeparateValues(t *testing.T) {
	args, err := ParseArgs([]string{"my_pipeline", "--config", "pipeline.yson", "--flow-bin", "/bin/flow_server"})
	require.NoError(t, err)
	require.Equal(t, Args{ConfigPath: "pipeline.yson", FlowBin: "/bin/flow_server"}, args)
}

func TestParseArgsAcceptsInlineValues(t *testing.T) {
	args, err := ParseArgs([]string{"my_pipeline", "--config=pipeline.yson", "--flow-bin=/bin/flow_server"})
	require.NoError(t, err)
	require.Equal(t, Args{ConfigPath: "pipeline.yson", FlowBin: "/bin/flow_server"}, args)
}

func TestParseArgsSkipsUnknownFlags(t *testing.T) {
	args, err := ParseArgs([]string{
		"my_pipeline",
		"--verbose",
		"--config", "pipeline.yson",
		"--threads=8",
		"--flow-bin", "/bin/flow_server",
	})
	require.NoError(t, err)
	require.Equal(t, "pipeline.yson", args.ConfigPath)
	require.Equal(t, "/bin/flow_server", args.FlowBin)
}

func TestParseArgsRequiresConfig(t *testing.T) {
	_, err := ParseArgs([]string{"my_pipeline", "--flow-bin", "/bin/flow_server"})
	require.ErrorIs(t, err, ErrMissingConfig)
}

func TestParseArgsRequiresFlowBin(t *testing.T) {
	_, err := ParseArgs([]string{"my_pipeline", "--config", "pipeline.yson"})
	require.ErrorIs(t, err, ErrMissingFlowBin)
}

func TestParseArgsRejectsDanglingFlag(t *testing.T) {
	_, err := ParseArgs([]string{"my_pipeline", "--config"})
	require.ErrorContains(t, err, "expects a value")
}

const vanillaConfigYSON = `{
	vanilla = {
		enable = %true;
	};
	spec = {
		resources = {
			CompanionManager = {
				resource_class_name = "NYT::NFlow::NCompanion::TCompanionManager";
				parameters = {
					run_process = %false;
				};
			};
			Throttler = {
				resource_class_name = "NYT::NFlow::TDistributedThrottler";
				parameters = {
					limit = 100;
				};
			};
		};
	};
}`

func enrichToMap(t *testing.T, config string, companionPath string) map[string]any {
	t.Helper()

	extended, err := Enrich([]byte(config), companionPath, nil)
	require.NoError(t, err)

	var out map[string]any
	require.NoError(t, yson.Unmarshal(extended, &out))
	return out
}

func resourceParameters(t *testing.T, config map[string]any, id string) map[string]any {
	t.Helper()

	resources, ok := asMap(asMap2(t, config, "spec")["resources"])
	require.True(t, ok)
	resource, ok := asMap(resources[id])
	require.True(t, ok)
	parameters, ok := asMap(resource["parameters"])
	require.True(t, ok)
	return parameters
}

func asMap2(t *testing.T, node map[string]any, key string) map[string]any {
	t.Helper()

	m, ok := asMap(node[key])
	require.True(t, ok)
	return m
}

func TestEnrichPointsCompanionResourceAtShippedBinary(t *testing.T) {
	config := enrichToMap(t, vanillaConfigYSON, "/build/my_pipeline")

	parameters := resourceParameters(t, config, "CompanionManager")
	require.Equal(t, map[string]any{"executable": "./" + CompanionFileName}, asMap2(t, parameters, "entrypoint"))
	require.Equal(t, true, yson.ValueOf(parameters["run_process"]))
}

func TestEnrichShipsThePipelineBinary(t *testing.T) {
	config := enrichToMap(t, vanillaConfigYSON, "/build/my_pipeline")

	worker := asMap2(t, asMap2(t, config, "vanilla"), "worker")
	localFiles := asMap2(t, worker, "local_files")
	require.Equal(t, "/build/my_pipeline", yson.ValueOf(localFiles[CompanionFileName]))
	require.EqualValues(t, companionWorkerPortCount, yson.ValueOf(worker["port_count"]))
}

func TestEnrichReservesCompanionPort(t *testing.T) {
	for _, portCount := range []int{1, 2, companionWorkerPortCount, 5} {
		t.Run(fmt.Sprint(portCount), func(t *testing.T) {
			config := enrichToMap(t, fmt.Sprintf(`{
				vanilla = {
					enable = %%true;
					worker = {
						port_count = %d;
					};
				};
				spec = {
					resources = {
						CompanionManager = {
							resource_class_name = "NYT::NFlow::NCompanion::TCompanionManager";
						};
					};
				};
			}`, portCount), "/build/my_pipeline")

			worker := asMap2(t, asMap2(t, config, "vanilla"), "worker")
			expected := portCount
			if expected < companionWorkerPortCount {
				expected = companionWorkerPortCount
			}
			require.EqualValues(t, expected, yson.ValueOf(worker["port_count"]))
		})
	}
}

func TestEnrichLeavesOtherResourcesAlone(t *testing.T) {
	config := enrichToMap(t, vanillaConfigYSON, "/build/my_pipeline")

	parameters := resourceParameters(t, config, "Throttler")
	require.NotContains(t, parameters, "entrypoint")
	require.NotContains(t, parameters, "run_process")
	require.EqualValues(t, 100, yson.ValueOf(parameters["limit"]))
}

func TestEnrichIgnoresNonVanillaLaunch(t *testing.T) {
	for name, config := range map[string]string{
		"no vanilla block": `{spec = {resources = {CompanionManager = {resource_class_name = "NYT::NFlow::NCompanion::TCompanionManager"; parameters = {}}}}}`,
		"vanilla disabled": `{vanilla = {enable = %false}; spec = {resources = {CompanionManager = {resource_class_name = "NYT::NFlow::NCompanion::TCompanionManager"; parameters = {}}}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			enriched := enrichToMap(t, config, "/build/my_pipeline")

			parameters := resourceParameters(t, enriched, "CompanionManager")
			require.NotContains(t, parameters, "entrypoint")
			require.NotContains(t, parameters, "run_process")
		})
	}
}

func TestEnrichCreatesMissingParameters(t *testing.T) {
	config := enrichToMap(t, `{
		vanilla = {enable = %true};
		spec = {resources = {CompanionManager = {
			resource_class_name = "NYT::NFlow::NCompanion::TCompanionManager";
		}}};
	}`, "/build/my_pipeline")

	parameters := resourceParameters(t, config, "CompanionManager")
	require.Equal(t, map[string]any{"executable": "./" + CompanionFileName}, asMap2(t, parameters, "entrypoint"))
}

func TestEnrichRejectsNonMapConfig(t *testing.T) {
	_, err := Enrich([]byte(`[1;2;3]`), "/build/my_pipeline", nil)
	require.ErrorIs(t, err, ErrMalformedConfig)
}

func TestEnrichRejectsMalformedYSON(t *testing.T) {
	_, err := Enrich([]byte(`{unterminated =`), "/build/my_pipeline", nil)
	require.Error(t, err)
}

func TestEnrichAddsRegisteredStreamSchemas(t *testing.T) {
	registered := map[string]schema.Schema{
		"visits": {
			Columns: []schema.Column{{Name: "key", Type: schema.TypeString}},
		},
	}

	extended, err := Enrich([]byte(`{spec = {streams = {keys = {schema = []}}}}`), "/build/my_pipeline", registered)
	require.NoError(t, err)

	var config map[string]any
	require.NoError(t, yson.Unmarshal(extended, &config))
	streams := asMap2(t, asMap2(t, config, "spec"), "streams")
	visits := asMap2(t, streams, "visits")

	var got schema.Schema
	raw, err := yson.Marshal(visits["schema"])
	require.NoError(t, err)
	require.NoError(t, yson.Unmarshal(raw, &got))
	require.Equal(t, registered["visits"].Columns, got.Columns)
	require.Contains(t, streams, "keys")
}

func TestEnrichKeepsMatchingConfiguredStreamSchema(t *testing.T) {
	registered := map[string]schema.Schema{
		"visits": {
			Columns: []schema.Column{{Name: "key", Type: schema.TypeString}},
		},
	}

	_, err := Enrich([]byte(`{
		spec = {
			streams = {
				visits = {
					schema = [{name = key; type = utf8}];
				};
			};
		};
	}`), "/build/my_pipeline", registered)
	require.NoError(t, err)
}

func TestEnrichRejectsConflictingConfiguredStreamSchema(t *testing.T) {
	registered := map[string]schema.Schema{
		"visits": {
			Columns: []schema.Column{{Name: "key", Type: schema.TypeString}},
		},
	}

	_, err := Enrich([]byte(`{
		spec = {
			streams = {
				visits = {
					schema = [{name = key; type = int64}];
				};
			};
		};
	}`), "/build/my_pipeline", registered)
	require.ErrorIs(t, err, ErrStreamSchemaConflict)
}
