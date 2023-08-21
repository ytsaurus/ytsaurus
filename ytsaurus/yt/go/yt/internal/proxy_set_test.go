package internal

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProxySet(t *testing.T) {
	var (
		updateResult []string
		updateErr    error
	)

	set := &ProxySet{
		UpdatePeriod:  time.Second,
		BanDuration:   time.Second,
		UpdateFn:      func() ([]string, error) { return updateResult, updateErr },
		ActiveSetSize: 5,
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

	updateErr = nil
	updateResult = []string{"a", "b", "c", "d"}

	// Wait current active proxies expire.
	time.Sleep(time.Second)

	// Trigger active set update.
	_, _ = set.PickRandom(context.Background())
	time.Sleep(time.Millisecond * 100)

	require.Equal(t, []string{"a", "b", "c", "d"}, pickRepeatedly())

	updateErr = nil
	updateResult = []string{"a", "b", "c", "d", "e", "f", "g"}

	// Wait current active proxies expire.
	time.Sleep(time.Second)

	// Trigger active set update.
	_, _ = set.PickRandom(context.Background())
	time.Sleep(time.Millisecond * 100)

	active := pickRepeatedly()
	require.Len(t, active, 5)

	proxy := active[0]
	set.BanProxy(proxy)

	active = pickRepeatedly()
	require.Len(t, active, 5)
	require.NotContains(t, proxy, active)

	updateResult = []string{"a", "b", "c"}

	time.Sleep(time.Second * 2)
	_, _ = set.PickRandom(context.Background())
	time.Sleep(time.Millisecond * 100)

	require.Equal(t, []string{"a", "b", "c"}, pickRepeatedly())
}
