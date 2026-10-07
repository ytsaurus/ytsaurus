package internal

import (
	"context"
	"errors"
	"sort"
	"strings"
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

func TestInferYPClusterFromHostName(t *testing.T) {
	cluster, ok := inferYPClusterFromHostName("rpc-proxy.sas.yp-c.yandex.net:9013")
	require.True(t, ok)
	require.Equal(t, "sas", cluster)

	_, ok = inferYPClusterFromHostName("localhost")
	require.False(t, ok)

	_, ok = inferYPClusterFromHostName("host..example.net")
	require.False(t, ok)

	_, ok = inferYPClusterFromHostName("host." + strings.Repeat("a", maxYPClusterNameSize+1) + ".example.net")
	require.False(t, ok)
}

func TestProxySetPreferLocal(t *testing.T) {
	proxyList := []string{
		"foreign-1.vla.example.net",
		"foreign-2.vla.example.net",
	}
	priorityProvider, cluster := NewYPClusterProxyPriorityProvider("client.sas.example.net")
	require.Equal(t, "sas", cluster)
	set := &ProxySet{
		UpdateFn:         func() ([]string, error) { return proxyList, nil },
		ActiveSetSize:    2,
		PriorityProvider: priorityProvider,
	}

	updateProxySet(set)
	require.Equal(t, 2, set.active[ProxyPriorityForeign].size())
	require.Zero(t, set.active[ProxyPriorityLocal].size())

	proxyList = append(proxyList, "local.sas.example.net")
	updateProxySet(set)
	require.Equal(t, 1, set.active[ProxyPriorityLocal].size())
	require.Equal(t, 1, set.active[ProxyPriorityForeign].size())
	require.Equal(t, 1, set.inactive[ProxyPriorityForeign].size())
}

func TestProxySetPreferLocalKeepsActiveForeignPeer(t *testing.T) {
	priorityProvider, _ := NewYPClusterProxyPriorityProvider("client.sas.example.net")
	for i := 0; i < 32; i++ {
		proxyList := []string{"foreign-1.vla.example.net"}
		set := &ProxySet{
			UpdateFn:         func() ([]string, error) { return proxyList, nil },
			ActiveSetSize:    2,
			PriorityProvider: priorityProvider,
		}
		updateProxySet(set)

		proxyList = append(proxyList, "local.sas.example.net", "foreign-2.vla.example.net")
		updateProxySet(set)

		require.ElementsMatch(t, []string{"local.sas.example.net"}, set.active[ProxyPriorityLocal].set)
		require.ElementsMatch(t, []string{"foreign-1.vla.example.net"}, set.active[ProxyPriorityForeign].set)
		require.ElementsMatch(t, []string{"foreign-2.vla.example.net"}, set.inactive[ProxyPriorityForeign].set)
	}
}

func TestProxySetPreferLocalWithFullActiveSet(t *testing.T) {
	proxyList := []string{
		"b.sas.yp-c.yandex.net",
		"c.sas.yp-c.yandex.net",
		"a.man.yp-c.yandex.net",
	}
	priorityProvider, _ := NewYPClusterProxyPriorityProvider("home.man.yp-c.yandex.net")
	set := &ProxySet{
		UpdateFn:         func() ([]string, error) { return proxyList, nil },
		ActiveSetSize:    3,
		PriorityProvider: priorityProvider,
	}
	updateProxySet(set)

	proxyList = append(proxyList, "d.sas.yp-c.yandex.net")
	updateProxySet(set)

	require.ElementsMatch(t, []string{"a.man.yp-c.yandex.net"}, set.active[ProxyPriorityLocal].set)
	require.ElementsMatch(t, []string{
		"b.sas.yp-c.yandex.net",
		"c.sas.yp-c.yandex.net",
	}, set.active[ProxyPriorityForeign].set)
	require.ElementsMatch(t, []string{"d.sas.yp-c.yandex.net"}, set.inactive[ProxyPriorityForeign].set)

	for i := 0; i < 100; i++ {
		proxy, ok := set.doPickRandom()
		require.True(t, ok)
		require.Equal(t, "a.man.yp-c.yandex.net", proxy)
	}
}

func TestProxySetPriorityAwarenessThreshold(t *testing.T) {
	proxyList := []string{
		"local-1.sas.example.net",
		"local-2.sas.example.net",
		"foreign-1.vla.example.net",
		"foreign-2.vla.example.net",
	}
	priorityProvider, _ := NewYPClusterProxyPriorityProvider("client.sas.example.net")
	set := &ProxySet{
		UpdateFn:         func() ([]string, error) { return proxyList, nil },
		ActiveSetSize:    len(proxyList),
		PriorityProvider: priorityProvider,
	}
	updateProxySet(set)

	for i := 0; i < 1024; i++ {
		proxy, ok := set.doPickRandom()
		require.True(t, ok)
		require.Contains(t, proxy, ".sas.")
	}

	set.MinPeerCountForPriorityAwareness = 3
	pickedLocal := false
	pickedForeign := false
	for i := 0; i < 1024; i++ {
		proxy, ok := set.doPickRandom()
		require.True(t, ok)
		pickedLocal = pickedLocal || strings.Contains(proxy, ".sas.")
		pickedForeign = pickedForeign || strings.Contains(proxy, ".vla.")
	}
	require.True(t, pickedLocal)
	require.True(t, pickedForeign)
}

func TestProxySetPowerOfTwoChoices(t *testing.T) {
	set := &ProxySet{
		UpdateFn:                        func() ([]string, error) { return []string{"a", "b"}, nil },
		ActiveSetSize:                   2,
		EnablePowerOfTwoChoicesStrategy: true,
	}
	updateProxySet(set)

	set.IncrementInflightRequestCount("a")
	for range 100 {
		proxy, ok := set.doPickRandom()
		require.True(t, ok)
		require.Equal(t, "b", proxy)
	}
	set.DecrementInflightRequestCount("a")

	set.IncrementInflightRequestCount("b")
	for range 100 {
		proxy, ok := set.doPickRandom()
		require.True(t, ok)
		require.Equal(t, "a", proxy)
	}
	set.DecrementInflightRequestCount("b")

	require.Empty(t, set.inflightRequestCount)
}

func TestProxySetPowerOfTwoChoicesTie(t *testing.T) {
	set := &ProxySet{
		UpdateFn:                        func() ([]string, error) { return []string{"a", "b"}, nil },
		ActiveSetSize:                   2,
		EnablePowerOfTwoChoicesStrategy: true,
	}
	updateProxySet(set)

	picked := map[string]bool{}
	for range 100 {
		proxy, ok := set.doPickRandom()
		require.True(t, ok)
		picked[proxy] = true
	}
	require.Equal(t, map[string]bool{"a": true, "b": true}, picked)
}

func TestProxySetPowerOfTwoChoicesWithSingleProxy(t *testing.T) {
	set := &ProxySet{
		UpdateFn:                        func() ([]string, error) { return []string{"a"}, nil },
		ActiveSetSize:                   1,
		EnablePowerOfTwoChoicesStrategy: true,
	}
	updateProxySet(set)

	set.IncrementInflightRequestCount("a")
	proxy, ok := set.doPickRandom()
	require.True(t, ok)
	require.Equal(t, "a", proxy)
	set.DecrementInflightRequestCount("a")
}

func TestProxySetPowerOfTwoChoicesRespectsPriority(t *testing.T) {
	t.Run("enough local proxies", func(t *testing.T) {
		proxyList := []string{
			"local-1.sas.example.net",
			"local-2.sas.example.net",
			"foreign.vla.example.net",
		}
		priorityProvider, _ := NewYPClusterProxyPriorityProvider("client.sas.example.net")
		set := &ProxySet{
			UpdateFn:                        func() ([]string, error) { return proxyList, nil },
			ActiveSetSize:                   len(proxyList),
			PriorityProvider:                priorityProvider,
			EnablePowerOfTwoChoicesStrategy: true,
		}
		updateProxySet(set)

		set.IncrementInflightRequestCount("local-1.sas.example.net")
		for range 100 {
			proxy, ok := set.doPickRandom()
			require.True(t, ok)
			require.Equal(t, "local-2.sas.example.net", proxy)
		}
		set.DecrementInflightRequestCount("local-1.sas.example.net")
	})

	t.Run("single local proxy", func(t *testing.T) {
		proxyList := []string{
			"local.sas.example.net",
			"foreign.vla.example.net",
		}
		priorityProvider, _ := NewYPClusterProxyPriorityProvider("client.sas.example.net")
		set := &ProxySet{
			UpdateFn:                        func() ([]string, error) { return proxyList, nil },
			ActiveSetSize:                   len(proxyList),
			PriorityProvider:                priorityProvider,
			EnablePowerOfTwoChoicesStrategy: true,
		}
		updateProxySet(set)

		set.IncrementInflightRequestCount("local.sas.example.net")
		for range 100 {
			proxy, ok := set.doPickRandom()
			require.True(t, ok)
			require.Equal(t, "foreign.vla.example.net", proxy)
		}
		set.DecrementInflightRequestCount("local.sas.example.net")
	})

	t.Run("priority awareness threshold", func(t *testing.T) {
		proxyList := []string{
			"local-1.sas.example.net",
			"local-2.sas.example.net",
			"foreign-1.vla.example.net",
			"foreign-2.vla.example.net",
		}
		priorityProvider, _ := NewYPClusterProxyPriorityProvider("client.sas.example.net")
		set := &ProxySet{
			UpdateFn:                         func() ([]string, error) { return proxyList, nil },
			ActiveSetSize:                    len(proxyList),
			PriorityProvider:                 priorityProvider,
			MinPeerCountForPriorityAwareness: 3,
			EnablePowerOfTwoChoicesStrategy:  true,
		}
		updateProxySet(set)

		pickedForeign := false
		pickedAllLocal := false
		for range 1024 {
			peers := set.pickActivePeers(2)
			require.NotEqual(t, peers[0], peers[1])

			foreignCount := 0
			for _, peer := range peers {
				if strings.Contains(peer, ".vla.") {
					foreignCount++
				}
			}
			require.LessOrEqual(t, foreignCount, 1)
			pickedForeign = pickedForeign || foreignCount == 1
			pickedAllLocal = pickedAllLocal || foreignCount == 0
		}
		require.True(t, pickedForeign)
		require.True(t, pickedAllLocal)
	})
}

func updateProxySet(set *ProxySet) {
	updateDone := make(chan struct{})
	set.updateProxies(updateDone)
	<-updateDone
}
