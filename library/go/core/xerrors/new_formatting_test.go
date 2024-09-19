package xerrors

import (
	"testing"

	"go.ytsaurus.tech/library/go/core/xerrors/assertxerrors"
)

func TestNewFormatting(t *testing.T) {
	constructor := func(t *testing.T) error {
		return New("new")
	}
	expected := assertxerrors.Expectations{
		ExpectedS: "new",
		ExpectedV: "new",
		Frames: assertxerrors.NewStackTraceModeExpectation(`
new
    go.ytsaurus.tech/library/go/core/xerrors.TestNewFormatting.func1
        library/go/core/xerrors/new_formatting_test.go:11
`,
		),
		Stacks: assertxerrors.NewStackTraceModeExpectation(`
new
    go.ytsaurus.tech/library/go/core/xerrors.TestNewFormatting.func1
        library/go/core/xerrors/new_formatting_test.go:11
    go.ytsaurus.tech/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/go.ytsaurus.tech/library/go/core/xerrors/assertxerrors/assertxerrors.go:83
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
`,
			3, 4, 5, 6,
		),
		StackThenFrames: assertxerrors.NewStackTraceModeExpectation(`
new
    go.ytsaurus.tech/library/go/core/xerrors.TestNewFormatting.func1
        library/go/core/xerrors/new_formatting_test.go:11
    go.ytsaurus.tech/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/go.ytsaurus.tech/library/go/core/xerrors/assertxerrors/assertxerrors.go:83
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
`,
			3, 4, 5, 6,
		),
		StackThenNothing: assertxerrors.NewStackTraceModeExpectation(`
new
    go.ytsaurus.tech/library/go/core/xerrors.TestNewFormatting.func1
        library/go/core/xerrors/new_formatting_test.go:11
    go.ytsaurus.tech/library/go/core/xerrors/assertxerrors.RunTestsPerMode.func1
        /home/sidh/devel/go/src/go.ytsaurus.tech/library/go/core/xerrors/assertxerrors/assertxerrors.go:83
    testing.tRunner
        /home/sidh/.ya/tools/v4/774223543/src/testing/testing.go:1127
`,
			3, 4, 5, 6,
		),
		Nothing: assertxerrors.NewStackTraceModeExpectation("new"),
	}
	assertxerrors.RunTestsPerMode(t, expected, constructor)
}
