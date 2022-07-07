package yt

import (
	"os"
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	check "gopkg.in/check.v1"
)

func Test(t *testing.T) { check.TestingT(t) }

func init() {

	cluster := os.Getenv("YT_PROXY")
	token := ""

	ytDriverConstructor := func() (storagedriver.StorageDriver, error) {
		return New(cluster, token, "//registry")
	}
	skipCheck := func() string {
		return ""
	}

	testsuites.RegisterSuite(ytDriverConstructor, skipCheck)
}
