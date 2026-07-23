//go:build linux || darwin
// +build linux darwin

// tvmtool recipe exists only for linux & darwin so we skip another OSes
package tvmtool_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/library/go/yandex/tvm"
	"go.ytsaurus.tech/library/go/yandex/tvm/tvmtool"
)

const (
	tvmToolPortFile               = "tvmtool.port"
	tvmToolAuthTokenFile          = "tvmtool.authtoken"
	userTicketFor1120000000038691 = "3:user" +
		":CA4Q__________9_GjUKCQijrpqRpdT-ARCjrpqRpdT-ARoMYmI6c2Vzc2lvbmlkGgl0ZXN0OnRlc3Qg0oXY" +
		"zAQoAw:A-YI2yhoD7BbGU80_dKQ6vm7XADdvgD2QUFCeTI3XZ4MS4N8iENvsNDvYwsW89-vLQPv9pYqn8jxx" +
		"awkvu_ZS2aAfpU8vXtnEHvzUQfes2kMjweRJE71cyX8B0VjENdXC5QAfGyK7Y0b4elTDJzw8b28Ro7IFFbNe" +
		"qgcPInXndY"
	userTicketFor100300WithLoc1170    = "3:user:CA4Q__________9_GjAKBAjMjwYQzI8GGgpzb21lOnNjb3BlINKF2MwEKAMyDXRlc3QtbG9naW4taWQ4kgk:FBtHtzg3ZTDvPJ-Kb3IJ0kEurPjU--SyXWOWSN7qa3NGgM0nOt2xLYZbqrza8AeuDJJRtcuS-D13qqYvakuWAwOQq00yKDy6QcPOBDyliunA3nukJYCHI32BbmB76w3gwi5W3ddH3RTwnkoKhMbchAjrzqFbND-lzJe4_xkzPus"
	userTicketFor100300WithDevType777 = "3:user:CA4Q__________9_GiQKBAjMjwYQzI8GINKF2MwEKAMyDXRlc3QtbG9naW4taWRAiQY:ARnCF90wpw3Pe3i0MFwn9G5Erpivm8DoCu40OTchS1Q532bcHDuaTBITJpMZpx-H0cDwsrVFHEOibgrGKsU74UZ8XBob3L_R5SVrpGmLmxiV-TIx4Ie9hQsE5o4gsnYaaKv2pOUxj4qSmX55pvLPm3MjjMTbFHRi2XZNpXZ_xRM"

	serviceTicketFor41_42 = "3:serv:CBAQ__________9_IgQIKRAq" +
		":VVXL3wkhpBHB7OXSeG0IhqM5AP2CP-gJRD31ksAb-q7pmssBJKtPNbH34BSyLpBllmM1dgOfwL8ICUOGUA3l" +
		"jOrwuxZ9H8ayfdrpM7q1-BVPE0sh0L9cd8lwZIW6yHejTe59s6wk1tG5MdSfncdaJpYiF3MwNHSRklNAkb6hx" +
		"vg"
	serviceTicketFor41_99 = "3:serv:CBAQ__________9_IgQIKRBj" +
		":PjJKDOsEk8VyxZFZwsVnKrW1bRyA82nGd0oIxnEFEf7DBTVZmNuxEejncDrMxnjkKwimrumV9POK4ptTo0ZPY" +
		"6Du9zHR5QxekZYwDzFkECVrv9YT2QI03odwZJX8_WCpmlgI8hUog_9yZ5YCYxrQpWaOwDXx4T7VVMwH_Z9YTZk"
)

var (
	srvTicketRe = regexp.MustCompile(`^3:serv:[A-Za-z0-9_\-]+:[A-Za-z0-9_\-]+$`)
)

func newTvmToolClient(src string, authToken ...string) (*tvmtool.Client, error) {
	raw, err := os.ReadFile(tvmToolPortFile)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(string(raw))
	if err != nil {
		return nil, err
	}

	var auth string
	if len(authToken) == 0 {
		rawToken, err := os.ReadFile(tvmToolAuthTokenFile)
		if err != nil {
			return nil, err
		}
		auth = string(rawToken)
	} else {
		auth = authToken[0]
	}

	return newTvmToolClientAtURL(src, fmt.Sprintf("http://localhost:%d", port), auth)
}

func newTvmToolClientAtURL(src string, apiURL string, authToken string) (*tvmtool.Client, error) {
	zlog, _ := zap.New(zap.ConsoleConfig(log.DebugLevel))

	return tvmtool.NewClient(
		apiURL,
		tvmtool.WithAuthToken(authToken),
		tvmtool.WithCacheEnabled(false),
		tvmtool.WithSrc(src),
		tvmtool.WithLogger(zlog),
	)
}

func TestNewClient(t *testing.T) {
	client, err := newTvmToolClient("main")
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClient_GetStatus(t *testing.T) {
	client, err := newTvmToolClient("main")
	require.NoError(t, err)
	status, err := client.GetStatus(context.Background())
	require.NoError(t, err, "ping must work")
	require.Equal(t, tvm.ClientOK, status.Status)
}

func TestClient_BadAuth(t *testing.T) {
	badClient, err := newTvmToolClient("main", "fake-auth")
	require.NoError(t, err)

	_, err = badClient.GetServiceTicketForAlias(context.Background(), "lala")
	require.Error(t, err)
	require.IsType(t, err, &tvmtool.Error{})
	srvTickerErr := err.(*tvmtool.Error)
	require.Equal(t, tvmtool.ErrorAuthFail, srvTickerErr.Code)
}

func TestClient_GetServiceTicket(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("invalid_alias", func(t *testing.T) {
		t.Parallel()
		_, err := tvmClient.GetServiceTicketForAlias(ctx, "not_exists")
		require.Error(t, err, "ticket for invalid alias must fails")
		assert.IsType(t, err, &tvmtool.Error{}, "must return tvm err")
		assert.EqualError(t, err, "tvm: code ErrorBadRequest: can't find in config destination tvmid for src = 42, dstparam = not_exists (strconv)")
	})

	t.Run("invalid_dst_id", func(t *testing.T) {
		t.Parallel()
		_, err := tvmClient.GetServiceTicketForID(ctx, 123123123)
		require.Error(t, err, "ticket for invalid ID must fails")
		assert.IsType(t, err, &tvmtool.Error{}, "must return tvm err")
		assert.EqualError(t, err, "tvm: code ErrorBadRequest: can't find in config destination tvmid for src = 42, dstparam = 123123123 (by number)")
	})

	t.Run("by_alias", func(t *testing.T) {
		// Try to get ticket by alias
		t.Parallel()
		heTicketByAlias, err := tvmClient.GetServiceTicketForAlias(ctx, "he")
		if assert.NoError(t, err, "failed to get srv ticket to 'he'") {
			assert.Regexp(t, srvTicketRe, heTicketByAlias, "invalid 'he' srv ticket")
		}

		heCloneTicketAlias, err := tvmClient.GetServiceTicketForAlias(ctx, "he_clone")
		if assert.NoError(t, err, "failed to get srv ticket to 'he_clone'") {
			assert.Regexp(t, srvTicketRe, heCloneTicketAlias, "invalid 'he_clone' srv ticket")
		}
	})

	t.Run("by_dst_id", func(t *testing.T) {
		// Try to get ticket by id
		t.Parallel()
		heTicketByID, err := tvmClient.GetServiceTicketForID(ctx, 100500)
		if assert.NoError(t, err, "failed to get srv ticket to '100500'") {
			assert.Regexp(t, srvTicketRe, heTicketByID, "invalid '100500' srv ticket")
		}
	})
}

func TestClient_GetServiceTicketV2(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("invalid_alias", func(t *testing.T) {
		// Ticket for invalid alias must fails
		t.Parallel()
		_, err := tvmClient.GetServiceTicket(ctx, "not_exists")
		require.Error(t, err, "ticket for invalid alias must fails")
		assert.IsType(t, err, &tvmtool.Error{}, "must return tvm err")
		assert.EqualError(t, err, "tvm: code ErrorBadRequest: can't find in config destination tvmid for src = main, dstparam = not_exists")
	})

	t.Run("by_alias", func(t *testing.T) {
		// Try to get ticket by alias
		t.Parallel()
		heTicketByAlias, err := tvmClient.GetServiceTicket(ctx, "he")
		if assert.NoError(t, err, "failed to get srv ticket to 'he'") {
			assert.Regexp(t, srvTicketRe, heTicketByAlias, "invalid 'he' srv ticket")
		}

		heCloneTicketAlias, err := tvmClient.GetServiceTicket(ctx, "he_clone")
		if assert.NoError(t, err, "failed to get srv ticket to 'he_clone'") {
			assert.Regexp(t, srvTicketRe, heCloneTicketAlias, "invalid 'he_clone' srv ticket")
		}
	})

	t.Run("override_invalid_src", func(t *testing.T) {
		// Try to get ticket by alias
		t.Parallel()

		tvmClient, err := newTvmToolClient("invalid")
		require.NoError(t, err)

		_, err = tvmClient.GetServiceTicket(ctx, "he")
		assert.Error(t, err, "given src is not valid, get srv ticket to 'he' must fail")

		heTicketByAlias, err := tvmClient.GetServiceTicket(ctx, "he", tvm.WithSrcOverride("main"))
		if assert.NoError(t, err, "failed to get srv ticket to 'he'") {
			assert.Regexp(t, srvTicketRe, heTicketByAlias, "invalid 'he' srv ticket")
		}

		_, err = tvmClient.GetServiceTicket(ctx, "he_clone")
		assert.Error(t, err, "given src is not valid, get srv ticket to 'he' must fail")

		heCloneTicketAlias, err := tvmClient.GetServiceTicket(ctx, "he_clone", tvm.WithSrcOverride("main"))
		if assert.NoError(t, err, "failed to get srv ticket to 'he_clone'") {
			assert.Regexp(t, srvTicketRe, heCloneTicketAlias, "invalid 'he_clone' srv ticket")
		}
	})
}

func TestClient_CheckServiceTicket(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	ctx := context.Background()
	t.Run("self_to_self", func(t *testing.T) {
		t.Parallel()

		// Check from self to self
		selfTicket, err := tvmClient.GetServiceTicketForAlias(ctx, "self")
		require.NoError(t, err, "failed to get service ticket to 'self'")
		assert.Regexp(t, srvTicketRe, selfTicket, "invalid 'self' srv ticket")

		// Now we can check srv ticket
		ticketInfo, err := tvmClient.CheckServiceTicket(ctx, selfTicket)
		require.NoError(t, err, "failed to check srv ticket main -> self")

		assert.Equal(t, tvm.ClientID(42), ticketInfo.SrcID)
		assert.NotEmpty(t, ticketInfo.LogInfo)
		assert.NotEmpty(t, ticketInfo.DbgInfo)
	})

	t.Run("to_another", func(t *testing.T) {
		t.Parallel()

		// Check from another client (41) to self
		ticketInfo, err := tvmClient.CheckServiceTicket(ctx, serviceTicketFor41_42)
		require.NoError(t, err, "failed to check srv ticket 41 -> 42")

		assert.Equal(t, tvm.ClientID(41), ticketInfo.SrcID)
		assert.NotEmpty(t, ticketInfo.LogInfo)
		assert.NotEmpty(t, ticketInfo.DbgInfo)
	})

	t.Run("invalid_dst", func(t *testing.T) {
		t.Parallel()

		// Check from another client (41) to invalid dst (99)
		ticketInfo, err := tvmClient.CheckServiceTicket(ctx, serviceTicketFor41_99)
		require.Error(t, err, "srv ticket for 41 -> 99 must fails")
		assert.NotEmpty(t, ticketInfo.LogInfo)
		assert.NotEmpty(t, ticketInfo.DbgInfo)

		ticketErr := err.(*tvmtool.TicketError)
		require.IsType(t, err, &tvmtool.TicketError{})
		assert.Equal(t, tvmtool.TicketErrorOther, ticketErr.Status)
		assert.Equal(t, "Wrong ticket dst, expected 42, got 99", ticketErr.Msg)
	})

	t.Run("broken", func(t *testing.T) {
		t.Parallel()

		// Check with broken sign
		_, err := tvmClient.CheckServiceTicket(ctx, "lalala")
		require.Error(t, err, "srv ticket with broken sign must fails")
		ticketErr := err.(*tvmtool.TicketError)
		require.IsType(t, err, &tvmtool.TicketError{})
		assert.Equal(t, tvmtool.TicketErrorOther, ticketErr.Status)
		assert.Equal(t, "invalid ticket format", ticketErr.Msg)
	})
}

func TestClient_MultipleClients(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	slaveClient, err := newTvmToolClient("slave")
	require.NoError(t, err)

	ctx := context.Background()

	ticket, err := tvmClient.GetServiceTicketForAlias(ctx, "slave")
	require.NoError(t, err, "failed to get service ticket to 'slave'")
	assert.Regexp(t, srvTicketRe, ticket, "invalid 'slave' srv ticket")

	ticketInfo, err := slaveClient.CheckServiceTicket(ctx, ticket)
	require.NoError(t, err, "failed to check srv ticket main -> self")

	assert.Equal(t, tvm.ClientID(42), ticketInfo.SrcID)
	assert.NotEmpty(t, ticketInfo.LogInfo)
	assert.NotEmpty(t, ticketInfo.DbgInfo)
}

func TestClient_CheckUserTicket(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	t.Run("check_user_ticket", func(t *testing.T) {
		t.Parallel()
		ticketInfo, err := tvmClient.CheckUserTicket(context.Background(), userTicketFor1120000000038691)
		require.NoError(t, err, "failed to check user ticket")

		assert.Equal(t, tvm.UID(1120000000038691), ticketInfo.DefaultUID)
		assert.Subset(t, []tvm.UID{1120000000038691}, ticketInfo.UIDs)
		assert.Equal(t, tvm.BlackboxTestYateam, ticketInfo.Env)
		assert.Subset(t, []string{"bb:sessionid", "test:test"}, ticketInfo.Scopes)
		assert.NotEmpty(t, ticketInfo.LogInfo)
		assert.NotEmpty(t, ticketInfo.DbgInfo)
		assert.EqualValues(t, 0, ticketInfo.Location)
		assert.EqualValues(t, 0, ticketInfo.DeviceType)
	})

	t.Run("check_user_ticket_with_location", func(t *testing.T) {
		t.Parallel()
		ticketInfo, err := tvmClient.CheckUserTicket(context.Background(), userTicketFor100300WithLoc1170)
		require.NoError(t, err, "failed to check user ticket")

		assert.Equal(t, tvm.UID(100300), ticketInfo.DefaultUID)
		assert.Subset(t, []tvm.UID{100300}, ticketInfo.UIDs)
		assert.Equal(t, tvm.BlackboxTestYateam, ticketInfo.Env)
		assert.Subset(t, []string{"some:scope"}, ticketInfo.Scopes)
		assert.NotEmpty(t, ticketInfo.LogInfo)
		assert.NotEmpty(t, ticketInfo.DbgInfo)
		assert.EqualValues(t, 1170, ticketInfo.Location)
	})

	t.Run("check_user_ticket_with_device_type", func(t *testing.T) {
		t.Parallel()
		ticketInfo, err := tvmClient.CheckUserTicket(context.Background(), userTicketFor100300WithDevType777)
		require.NoError(t, err, "failed to check user ticket")

		assert.Equal(t, tvm.UID(100300), ticketInfo.DefaultUID)
		assert.Subset(t, []tvm.UID{100300}, ticketInfo.UIDs)
		assert.Equal(t, tvm.BlackboxTestYateam, ticketInfo.Env)
		assert.NotEmpty(t, ticketInfo.LogInfo)
		assert.NotEmpty(t, ticketInfo.DbgInfo)
		assert.EqualValues(t, 777, ticketInfo.DeviceType)
	})
}

func TestClient_CheckTicketV2(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	ctx := context.Background()
	t.Run("service_ticket_self_to_self", func(t *testing.T) {
		t.Parallel()

		// Check from self to self
		selfTicket, err := tvmClient.GetServiceTicket(ctx, "self")
		require.NoError(t, err, "failed to get service ticket to 'self'")
		assert.Regexp(t, srvTicketRe, selfTicket, "invalid 'self' srv ticket")

		// Now we can check srv ticket
		ticketInfo, err := tvmClient.CheckTicket(ctx, selfTicket, "")
		require.NoError(t, err, "failed to check srv ticket main -> self")

		assert.Equal(t, tvm.ClientID(42), ticketInfo.Service.SrcID)
		assert.NotEmpty(t, ticketInfo.Service.LogInfo)
		assert.NotEmpty(t, ticketInfo.Service.DbgInfo)
		assert.Empty(t, ticketInfo.User, "must be no user ticket")
	})
	t.Run("service_ticket_to_another", func(t *testing.T) {
		t.Parallel()

		// Check from another client (41) to self
		ticketInfo, err := tvmClient.CheckTicket(ctx, serviceTicketFor41_42, "")
		require.NoError(t, err, "failed to check srv ticket 41 -> 42")

		assert.Equal(t, tvm.ClientID(41), ticketInfo.Service.SrcID)
		assert.NotEmpty(t, ticketInfo.Service.LogInfo)
		assert.NotEmpty(t, ticketInfo.Service.DbgInfo)
		assert.Empty(t, ticketInfo.User, "must be no user ticket")
	})

	t.Run("service_ticket_invalid_dst", func(t *testing.T) {
		t.Parallel()

		// Check from another client (41) to invalid dst (99)
		ticketInfo, err := tvmClient.CheckTicket(ctx, serviceTicketFor41_99, "")
		require.Error(t, err, "srv ticket for 41 -> 99 must fails")
		assert.NotEmpty(t, ticketInfo.Service.LogInfo)
		assert.NotEmpty(t, ticketInfo.Service.DbgInfo)
		assert.Empty(t, ticketInfo.User, "must be no user ticket")

		ticketErr := err.(*tvmtool.TicketError)
		require.IsType(t, err, &tvmtool.TicketError{})
		assert.Equal(t, tvmtool.TicketErrorOther, ticketErr.Status)
		assert.Equal(t, "invalid service ticket", ticketErr.Msg)
	})

	t.Run("service_ticket_broken", func(t *testing.T) {
		t.Parallel()

		// Check with broken sign
		_, err := tvmClient.CheckTicket(ctx, "lalala", "")
		require.Error(t, err, "srv ticket with broken sign must fails")
		ticketErr := err.(*tvmtool.TicketError)
		require.IsType(t, err, &tvmtool.TicketError{})
		assert.Equal(t, tvmtool.TicketErrorOther, ticketErr.Status)
		assert.Equal(t, "invalid service ticket", ticketErr.Msg)
	})

	t.Run("user_ticket_valid", func(t *testing.T) {
		ticketInfo, err := tvmClient.CheckTicket(ctx, "", userTicketFor1120000000038691)
		require.NoError(t, err, "failed to check user ticket")

		assert.Equal(t, tvm.UID(1120000000038691), ticketInfo.User.DefaultUID)
		assert.Subset(t, []tvm.UID{1120000000038691}, ticketInfo.User.UIDs)
		assert.Subset(t, []string{"bb:sessionid", "test:test"}, ticketInfo.User.Scopes)
		assert.NotEmpty(t, ticketInfo.User.LogInfo)
		assert.NotEmpty(t, ticketInfo.User.DbgInfo)
		assert.Empty(t, ticketInfo.Service, "must be no service ticket")
		assert.EqualValues(t, 0, ticketInfo.User.Location)
		assert.EqualValues(t, 0, ticketInfo.User.DeviceType)
		assert.EqualValues(t, tvm.BlackboxTestYateam, ticketInfo.User.Env)
	})
	t.Run("user_ticket_with_location", func(t *testing.T) {
		ticketInfo, err := tvmClient.CheckTicket(ctx, "", userTicketFor100300WithLoc1170)
		require.NoError(t, err, "failed to check user ticket")

		assert.Equal(t, tvm.UID(100300), ticketInfo.User.DefaultUID)
		assert.Subset(t, []tvm.UID{100300}, ticketInfo.User.UIDs)
		assert.Subset(t, []string{"some:scope"}, ticketInfo.User.Scopes)
		assert.NotEmpty(t, ticketInfo.User.LogInfo)
		assert.NotEmpty(t, ticketInfo.User.DbgInfo)
		assert.EqualValues(t, 1170, ticketInfo.User.Location)
		assert.EqualValues(t, tvm.BlackboxTestYateam, ticketInfo.User.Env)
		assert.Empty(t, ticketInfo.Service, "must be no service ticket")
	})
	t.Run("user_ticket_with_device_type", func(t *testing.T) {
		ticketInfo, err := tvmClient.CheckTicket(ctx, "", userTicketFor100300WithDevType777)
		require.NoError(t, err, "failed to check user ticket")

		assert.Equal(t, tvm.UID(100300), ticketInfo.User.DefaultUID)
		assert.Subset(t, []tvm.UID{100300}, ticketInfo.User.UIDs)
		assert.NotEmpty(t, ticketInfo.User.LogInfo)
		assert.NotEmpty(t, ticketInfo.User.DbgInfo)
		assert.EqualValues(t, 777, ticketInfo.User.DeviceType)
		assert.EqualValues(t, tvm.BlackboxTestYateam, ticketInfo.User.Env)
		assert.Empty(t, ticketInfo.Service, "must be no service ticket")
	})
	t.Run("service_and_user_tickets", func(t *testing.T) {
		t.Parallel()

		// Check from self to self
		selfTicket, err := tvmClient.GetServiceTicket(ctx, "self")
		require.NoError(t, err, "failed to get service ticket to 'self'")
		assert.Regexp(t, srvTicketRe, selfTicket, "invalid 'self' srv ticket")

		// Now we can check srv ticket
		ticketInfo, err := tvmClient.CheckTicket(ctx, selfTicket, userTicketFor1120000000038691)
		require.NoError(t, err, "failed to check srv ticket main -> self and user ticket")

		assert.Equal(t, tvm.ClientID(42), ticketInfo.Service.SrcID)
		assert.NotEmpty(t, ticketInfo.Service.LogInfo)
		assert.NotEmpty(t, ticketInfo.Service.DbgInfo)
		assert.NotEmpty(t, ticketInfo.User.LogInfo)
		assert.NotEmpty(t, ticketInfo.User.DbgInfo)
		assert.NotEmpty(t, ticketInfo.User.Env)

	})
}

func TestClient_Version(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	version, err := tvmClient.Version(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, version)
}

func TestClient_GetRoles(t *testing.T) {
	// ходить в настоящий tvmtool не получилось,
	// потому что он не может стартовать в рецепте с "roles_for_idm_slug"
	tvmtoolServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		require.Equal(t, "main", req.URL.Query().Get("self"))
		if req.Header.Get("If-None-Match") == "\"GY2GCMTFMQ2DE\"" {
			res.WriteHeader(http.StatusNotModified)
			return
		}
		res.WriteHeader(http.StatusOK)
		resp := `{"revision":"GY2GCMTFMQ2DE","born_date":1688399170,"user":{"1120000000022901":{"/role/advanced/":[{}]}}}`
		_, err := res.Write([]byte(resp))
		require.NoError(t, err)
	}))
	defer tvmtoolServer.Close()

	tvmClient, err := newTvmToolClientAtURL("main", tvmtoolServer.URL, "12345")
	require.NoError(t, err)

	// первый раз отвечаем с непустыми данными из tvmtool
	// второй раз отвечаем из кеша
	for i := 0; i < 2; i++ {
		roles, err := tvmClient.GetRoles(context.Background())
		require.NoError(t, err)
		require.NotNil(t, roles)

		userRoles, err := roles.GetRolesForUser(&tvm.CheckedUserTicket{
			DefaultUID: 1120000000022901,
			Env:        tvm.BlackboxProdYateam,
		}, nil)
		require.NoError(t, err)
		require.True(t, userRoles.HasRole("/role/advanced/"))
	}
}

func TestClient_Errors(t *testing.T) {
	newSimpleServer := func(replier func(res http.ResponseWriter)) func() *httptest.Server {
		return func() *httptest.Server {
			srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				replier(res)
			}))
			srv.Config.ErrorLog = stdlog.New(io.Discard, "", 0)

			return srv
		}
	}

	type testCase struct {
		ctx          func() context.Context
		srv          func() *httptest.Server
		errCode      tvm.ErrorCode
		errRetriable bool
	}
	cases := map[string]testCase{
		"unautorized": {
			ctx: context.Background,
			srv: newSimpleServer(func(res http.ResponseWriter) {
				res.WriteHeader(http.StatusUnauthorized)
			}),
			errCode:      tvm.ErrorAuthFail,
			errRetriable: false,
		},
		"bad_req": {
			ctx: context.Background,
			srv: newSimpleServer(func(res http.ResponseWriter) {
				res.WriteHeader(http.StatusBadRequest)
			}),
			errCode:      tvm.ErrorBadRequest,
			errRetriable: false,
		},
		"bad_gw": {
			ctx: context.Background,
			srv: newSimpleServer(func(res http.ResponseWriter) {
				res.WriteHeader(http.StatusBadGateway)
			}),
			errCode:      tvm.ErrorOther,
			errRetriable: false,
		},
		"internal_error": {
			ctx: context.Background,
			srv: newSimpleServer(func(res http.ResponseWriter) {
				res.WriteHeader(http.StatusInternalServerError)
			}),
			errCode:      tvm.ErrorOther,
			errRetriable: true,
		},
		"not_started": {
			ctx: context.Background,
			srv: func() *httptest.Server {
				srv := newSimpleServer(func(_ http.ResponseWriter) {
					panic("should not be called")
				})()
				srv.Close()
				return srv
			},
			errCode:      tvm.ErrorOther,
			errRetriable: true,
		},
		"slowpoke": {
			ctx: context.Background,
			srv: newSimpleServer(func(res http.ResponseWriter) {
				time.Sleep(5 * time.Second)
				res.WriteHeader(http.StatusInternalServerError)
			}),
			errCode:      tvm.ErrorOther,
			errRetriable: true,
		},
		"slowpoke_client": {
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			srv: newSimpleServer(func(res http.ResponseWriter) {
				time.Sleep(5 * time.Second)
				res.WriteHeader(http.StatusInternalServerError)
			}),
			errCode:      tvm.ErrorOther,
			errRetriable: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := tc.srv()
			defer srv.Close()

			tvmClient, err := newTvmToolClientAtURL("main", srv.URL, "12345")
			require.NoError(t, err)

			errChecker := func(t *testing.T, err error) {
				require.Error(t, err)

				var tvmErr *tvm.Error
				isTVMErr := errors.As(err, &tvmErr)
				if errors.Is(err, context.Canceled) {
					require.Falsef(t, isTVMErr, "unexptected error type %T: %v", err, err)
					return
				}

				require.Truef(t, isTVMErr, "unexptected error type %T: %v", err, err)
				require.Equalf(t, tc.errCode, tvmErr.Code, "unexpected error code: %s", tvmErr.Code)
				require.Equal(t, tc.errRetriable, tvmErr.Retriable)
			}

			t.Run("get_srv", func(t *testing.T) {
				_, err := tvmClient.GetServiceTicket(tc.ctx(), "kek")
				errChecker(t, err)
			})

			t.Run("check_srv", func(t *testing.T) {
				_, err := tvmClient.CheckServiceTicket(tc.ctx(), "kek")
				errChecker(t, err)
			})

			t.Run("check_user", func(t *testing.T) {
				_, err := tvmClient.CheckUserTicket(tc.ctx(), "kek")
				errChecker(t, err)
			})

			t.Run("check_ticket", func(t *testing.T) {
				_, err := tvmClient.CheckTicket(tc.ctx(), "lol", "kek")
				errChecker(t, err)
			})
		})
	}
}

func TestClient_ErrorsUnwrap(t *testing.T) {
	type testCase struct {
		ctx     func() context.Context
		replier func(res http.ResponseWriter)
		checker func(t *testing.T, err error)
	}
	cases := map[string]testCase{
		"internal_error": {
			ctx: context.Background,
			replier: func(res http.ResponseWriter) {
				res.WriteHeader(http.StatusInternalServerError)
			},
			checker: func(t *testing.T, err error) {
				require.Error(t, err)

				var urlErr *url.Error
				require.Falsef(t, errors.As(err, &urlErr), "internal error moust not have url.Error in the chain")

				var tvmErr *tvm.Error
				require.ErrorAs(t, err, &tvmErr)
			},
		},
		"slowpoke": {
			ctx: context.Background,
			replier: func(res http.ResponseWriter) {
				time.Sleep(5 * time.Second)
				res.WriteHeader(http.StatusInternalServerError)
			},
			checker: func(t *testing.T, err error) {
				require.Error(t, err)

				var urlErr *url.Error
				require.ErrorAs(t, err, &urlErr)

				var tvmErr *tvm.Error
				require.ErrorAs(t, err, &tvmErr)
			},
		},
		"slowpoke_client": {
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			replier: func(res http.ResponseWriter) {
				time.Sleep(5 * time.Second)
				res.WriteHeader(http.StatusInternalServerError)
			},
			checker: func(t *testing.T, err error) {
				require.Error(t, err)

				require.ErrorIs(t, err, context.Canceled)
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				tc.replier(res)
			}))
			srv.Config.ErrorLog = stdlog.New(io.Discard, "", 0)
			defer srv.Close()

			tvmClient, err := newTvmToolClientAtURL("main", srv.URL, "12345")
			require.NoError(t, err)

			t.Run("get_srv", func(t *testing.T) {
				_, err := tvmClient.GetServiceTicket(tc.ctx(), "kek")
				tc.checker(t, err)
			})

			t.Run("check_srv", func(t *testing.T) {
				_, err := tvmClient.CheckServiceTicket(tc.ctx(), "kek")
				tc.checker(t, err)
			})

			t.Run("check_user", func(t *testing.T) {
				_, err := tvmClient.CheckUserTicket(tc.ctx(), "kek")
				tc.checker(t, err)
			})

			t.Run("check_ticket", func(t *testing.T) {
				_, err := tvmClient.CheckTicket(tc.ctx(), "lol", "kek")
				tc.checker(t, err)
			})
		})
	}
}

func TestClient_CacheUsage(t *testing.T) {
	type testCase struct {
		bornDate      string
		mustCache     bool
		mustReRequest bool
	}
	cases := map[string]testCase{
		"ok": {
			bornDate:      strconv.FormatInt(time.Now().Unix(), 10),
			mustCache:     true,
			mustReRequest: false,
		},
		"ok_no_date": {
			bornDate:      "",
			mustCache:     true,
			mustReRequest: false,
		},
		"invalid_date": {
			bornDate:      "blah-blah",
			mustCache:     true,
			mustReRequest: false,
		},
		"gonna_miss": {
			bornDate: strconv.FormatInt(
				time.Now().Add(-tvmtool.CacheTTL).Add(-time.Second).Unix(),
				10,
			),
			mustCache:     true,
			mustReRequest: true,
		},
		"miss": {
			bornDate: strconv.FormatInt(
				time.Now().Add(-tvmtool.CacheMaxTTL).Add(-time.Second).Unix(),
				10,
			),
			mustCache:     false,
			mustReRequest: true,
		},
	}

	newClient := func(t *testing.T, url string) *tvmtool.Client {
		tvmClient, err := tvmtool.NewClient(
			url,
			tvmtool.WithCacheEnabled(true),
			tvmtool.WithSrc("main"),
		)
		require.NoError(t, err)

		return tvmClient
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var toolHits int32
			srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				atomic.AddInt32(&toolHits, 1)
				res.Header().Set("Content-Type", "application/json")
				if tc.bornDate != "" {
					res.Header().Set("X-Ya-Tvmtool-Data-Birthtime", tc.bornDate)
				}

				res.WriteHeader(http.StatusOK)
				_, _ = res.Write([]byte(`{"tst": {"ticket": "3:serv:some_ticket","tvm_id": 123}}`))
			}))
			srv.Config.ErrorLog = stdlog.New(io.Discard, "", 0)
			defer srv.Close()

			testFn := func(t *testing.T, fn func(tvmClient *tvmtool.Client) (string, error)) {
				tvmc := newClient(t, srv.URL)

				atomic.StoreInt32(&toolHits, 0)
				cacheSize := 0
				if tc.mustCache {
					cacheSize = 1
				}

				for i := 0; i < 3; i++ {
					t.Run(fmt.Sprintf("fetch_%d", i), func(t *testing.T) {
						ticket, err := fn(tvmc)
						require.NoError(t, err)
						require.Equal(t, "3:serv:some_ticket", ticket)
						require.Len(t, tvmc.Cache().Aliases(), cacheSize)
					})
				}

				t.Run("check_hits", func(t *testing.T) {
					if tc.mustReRequest {
						require.Equal(t, int32(3), atomic.LoadInt32(&toolHits))
						return
					}

					require.Equal(t, int32(1), atomic.LoadInt32(&toolHits))
				})
			}

			t.Run("get_srv", func(t *testing.T) {
				testFn(t, func(tvmClient *tvmtool.Client) (string, error) {
					return tvmClient.GetServiceTicket(context.Background(), "tst")
				})
			})

			t.Run("get_srv_for_id", func(t *testing.T) {
				testFn(t, func(tvmClient *tvmtool.Client) (string, error) {
					return tvmClient.GetServiceTicketForID(context.Background(), tvm.ClientID(123))
				})
			})

			t.Run("get_srv_alias", func(t *testing.T) {
				testFn(t, func(tvmClient *tvmtool.Client) (string, error) {
					return tvmClient.GetServiceTicketForAlias(context.Background(), "tst")
				})
			})
		})
	}
}
