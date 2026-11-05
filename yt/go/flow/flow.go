package flow

import (
	"os"

	"go.ytsaurus.tech/yt/go/flow/runner"
	"go.ytsaurus.tech/yt/go/schema"
)

// Pipeline is the entry point of a Go Flow pipeline binary.
type Pipeline struct {
	computations []*Computation
	streams      []Stream
}

// NewPipeline returns a pipeline with no computations.
func NewPipeline() *Pipeline {
	return &Pipeline{}
}

// Add registers computations to serve. Duplicate ids are reported when the server is built.
func (p *Pipeline) Add(computations ...*Computation) {
	p.computations = append(p.computations, computations...)
}

// AddStreams registers schemas that the runner adds to the pipeline spec.
func (p *Pipeline) AddStreams(streams ...Stream) {
	p.streams = append(p.streams, streams...)
}

// Computations returns the registered computations in registration order.
func (p *Pipeline) Computations() []*Computation {
	return p.computations
}

// Server builds the configured companion server.
func (p *Pipeline) Server(opts ...ServerOption) (*Server, error) {
	config, err := LoadConfig()
	if err != nil {
		return nil, err
	}

	server := NewServer(config, opts...)
	if err := server.Register(p.computations...); err != nil {
		return nil, err
	}
	return server, nil
}

// Run launches the pipeline or serves it as a companion, based on the environment.
func (p *Pipeline) Run(opts ...ServerOption) error {
	_, modeSet := os.LookupEnv(ModeEnvVar)
	_, configSet := os.LookupEnv(ConfigEnvVar)

	if !modeSet && !configSet {
		args, err := runner.ParseArgs(os.Args)
		if err != nil {
			return err
		}
		streamSchemas := make(map[string]schema.Schema, len(p.streams))
		for _, stream := range p.streams {
			streamSchemas[stream.ID] = stream.Schema.Table()
		}
		return runner.Launch(args, streamSchemas)
	}

	server, err := p.Server(opts...)
	if err != nil {
		return err
	}
	return server.Start()
}
