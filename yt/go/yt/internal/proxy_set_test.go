package internal

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProxySet(t *testing.T) {
	var (
		updateResult []string
		updateErr    error
	)

	set := &ProxySet{
		UpdateFn: func() ([]string, error) { return updateResult, updateErr },
	}

	_, err := set.PickRandom(context.Background())
	require.Equal(t, errProxyListEmpty, err)

	updateErr = errors.New("connection error")
	_, err = set.PickRandom(context.Background())
	require.Equal(t, updateErr, err)

	updateErr = nil
	updateResult = []string{"a", "b", "c"}

	pickRepeatedly := func() []string {
		proxies := map[string]struct{}{}

		for i := 0; i < 1024; i++ {
			name, err := set.PickRandom(context.Background())
			require.NoError(t, err)
			proxies[name] = struct{}{}
		}

		var proxyList []string
		for name := range proxies {
			proxyList = append(proxyList, name)
		}
		sort.Strings(proxyList)
		return proxyList
	}

	require.Equal(t, []string{"a", "b", "c"}, pickRepeatedly())

	set.BanProxy("b")
	require.Equal(t, []string{"a", "c"}, pickRepeatedly())

	set.BanProxy("c")
	require.Equal(t, []string{"a"}, pickRepeatedly())

	set.BanProxy("a")
	require.Equal(t, []string{"a", "b", "c"}, pickRepeatedly())
}
