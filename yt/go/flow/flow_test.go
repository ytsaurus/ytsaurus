package flow

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow/runner"
)

func noopRow(context.Context, Runtime, ExtendedMessage, OutputCollector) error { return nil }

func TestPipelineKeepsRegistrationOrder(t *testing.T) {
	p := NewPipeline()
	require.Empty(t, p.Computations())

	mapper := NewRowComputation("mapper", RowFunc(noopRow))
	reader := NewRowSourceComputation("reader", RowFunc(noopRow))
	p.Add(mapper)
	p.Add(reader)

	require.Equal(t, []*Computation{mapper, reader}, p.Computations())
}

func TestPipelineAddAcceptsSeveralAtOnce(t *testing.T) {
	p := NewPipeline()
	first := NewRowComputation("first", RowFunc(noopRow))
	second := NewBatchComputation("second", BatchFunc(func(context.Context, Runtime, []ExtendedMessage, OutputCollector) error {
		return nil
	}))

	p.Add(first, second)

	require.Equal(t, []*Computation{first, second}, p.Computations())
}

func TestPipelineCarriesComputationTypes(t *testing.T) {
	p := NewPipeline()
	p.Add(
		NewRowComputation("mapper", RowFunc(noopRow)),
		NewRowSourceComputation("reader", RowFunc(noopRow)),
	)

	computations := p.Computations()
	require.Equal(t, computationTypeTransform, computations[0].typ)
	require.Equal(t, computationTypeSource, computations[1].typ)
}

func TestPipelineServerRegistersComputations(t *testing.T) {
	t.Setenv(ModeEnvVar, WorkerMode)
	t.Setenv(ConfigEnvVar, fullConfigYSON)

	p := NewPipeline()
	p.Add(NewRowComputation("mapper", RowFunc(noopRow)))

	server, err := p.Server()
	require.NoError(t, err)
	require.Equal(t, 4242, server.Config().Port)
}

func TestPipelineServerRejectsDuplicateIDs(t *testing.T) {
	t.Setenv(ModeEnvVar, WorkerMode)
	t.Setenv(ConfigEnvVar, fullConfigYSON)

	p := NewPipeline()
	p.Add(
		NewRowComputation("mapper", RowFunc(noopRow)),
		NewRowComputation("mapper", RowFunc(noopRow)),
	)

	_, err := p.Server()
	require.Error(t, err)
}

func TestPipelineServerPropagatesConfigError(t *testing.T) {
	unsetEnv(t, ModeEnvVar)
	unsetEnv(t, ConfigEnvVar)
	t.Setenv(ModeEnvVar, "Controller")
	t.Setenv(ConfigEnvVar, fullConfigYSON)

	p := NewPipeline()
	p.Add(NewRowComputation("mapper", RowFunc(noopRow)))

	_, err := p.Server()
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestRunWithoutCompanionEnvironmentLaunches(t *testing.T) {
	unsetEnv(t, ModeEnvVar)
	unsetEnv(t, ConfigEnvVar)

	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"my_pipeline"}

	err := NewPipeline().Run()
	require.ErrorIs(t, err, runner.ErrMissingConfig)
}

func TestRunRefusesAHalfConfiguredCompanion(t *testing.T) {
	unsetEnv(t, ModeEnvVar)
	unsetEnv(t, ConfigEnvVar)
	t.Setenv(ModeEnvVar, WorkerMode)

	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"my_pipeline"}

	err := NewPipeline().Run()
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.NotErrorIs(t, err, runner.ErrMissingConfig)
}
