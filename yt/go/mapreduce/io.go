package mapreduce

import (
	"fmt"
	"os"
	"strconv"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/yson"
)

type jobContext struct {
	vault     map[string]string
	jobCookie int

	in  Reader
	out []*writer
}

func (c *jobContext) LookupVault(name string) (value string, ok bool) {
	value, ok = c.vault[name]
	return
}

func (c *jobContext) JobCookie() int {
	return c.jobCookie
}

func (c *jobContext) initEnv() error {
	c.vault = map[string]string{}

	vaultValue := os.Getenv("YT_SECURE_VAULT")
	if vaultValue != "" {
		if err := yson.Unmarshal([]byte(vaultValue), &c.vault); err != nil {
			return xerrors.Errorf("corrupted secure vault: %w", err)
		}
	}

	jobCookie := os.Getenv("YT_JOB_COOKIE")
	if jobCookie != "" {
		if cookie, err := strconv.Atoi(jobCookie); err != nil {
			return xerrors.Errorf("corrupted job cookie: %w", err)
		} else {
			c.jobCookie = cookie
		}
	}

	return nil
}

func (c *jobContext) onError(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
	os.Exit(1)
}

func (c *jobContext) finish() error {
	// TODO(prime@): return this check
	//if yc.in.err != nil {
	//	return xerrors.Errorf("input reader error: %w", yc.in.err)
	//}

	for _, out := range c.out {
		_ = out.Close()

		if out.err != nil {
			return xerrors.Errorf("output writer error: %w", out.err)
		}
	}

	return nil
}

func (c *jobContext) writers() (out []Writer) {
	for _, w := range c.out {
		out = append(out, w)
	}

	return
}
