package tooltest

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/library/go/yandex/tvm"
	"go.ytsaurus.tech/library/go/yandex/tvm/tvmauth"
)

const serviceTicketValid = "3:serv:CBAQ__________9_IgQIexAq:CeYHbo4MJQajoJluQVkCs8KPZGh454Xqdk8wBylfa_2xmQ2euarOVOGIg4q9OULydSXghWWcbhCMfJiNkp3ALeFVA0HctTjowNqbi5Kg8LesNQbbLeEn4DBX2psrxn9Ifu_ZHFAErj548jWB6ajicWsNNLXSF7RNH1I6kg98_WM"
const serviceTicketValidAdditional = "3:serv:CBAQ__________9_IgQIexAr:De6Bq6pX80tjCgb9WA0Fl0-FiwFJ5ArIWsJ_7AMS0w80N8tknc5ds0EfCQ5Gj_u1Uhptx8GMmUT3tSYRADJaCANk3gH4lAUpXN0Huak-E9Q60jL-Il68rBz-ATG2ClheErvDv-XFy07S_zycYbKlULOgN8i6eSW6aBAjE_mXvg0"
const serviceTicketInvalid = "3:serv:CBAQ__________9_IgYIKhCWkQY:DnbhBOAMpunP9TuhCvXV8Hg9MEUHSFbRETf710eHVS7plghVsdM-JlLR6XtGeiofX3yiCFMs4Nq7aFJqZwX75HFgGiQymyWWKm2pWTyF0pp8QnaTivIM-Q6xmMqfInUlYrozhkVPmIxT4fqsdrKEACq-Zh8VtuNQYrTLZgsUfWo"

func recipeToolOptions(t *testing.T) tvmauth.TvmToolSettings {
	var portStr, token []byte
	portStr, err := os.ReadFile("tvmtool.port")
	require.NoError(t, err)

	var port int
	port, err = strconv.Atoi(string(portStr))
	require.NoError(t, err)

	token, err = os.ReadFile("tvmtool.authtoken")
	require.NoError(t, err)

	return tvmauth.TvmToolSettings{Alias: "me", Port: port, AuthToken: string(token)}
}

func disableDstCheckOptions(t *testing.T) tvmauth.TvmToolSettings {
	s := recipeToolOptions(t)
	s.DisableDstCheck = true
	return s
}

func TestToolClient(t *testing.T) {
	c, err := tvmauth.NewToolClient(recipeToolOptions(t), &nop.Logger{})
	require.NoError(t, err)
	defer c.Destroy()

	t.Run("GetServiceTicketForID", func(t *testing.T) {
		_, err := c.GetServiceTicketForID(context.Background(), 100500)
		require.NoError(t, err)
	})

	t.Run("GetInvalidTicket", func(t *testing.T) {
		_, err := c.GetServiceTicketForID(context.Background(), 100999)
		require.Error(t, err)
		require.IsType(t, &tvm.Error{}, err)
		require.Equal(t, tvm.ErrorBrokenTvmClientSettings, err.(*tvm.Error).Code)
	})

	t.Run("ClientStatus", func(t *testing.T) {
		status, err := c.GetStatus(context.Background())
		require.NoError(t, err)

		t.Logf("Got client status: %v", status)

		require.Equal(t, tvm.ClientStatus(0), status.Status)
		require.Equal(t, "OK", status.LastError)
	})
}

func TestAdditionalSelfTvmIdCheck(t *testing.T) {
	c, err := tvmauth.NewToolClient(recipeToolOptions(t), &nop.Logger{})
	require.NoError(t, err)
	defer c.Destroy()

	ticketS, err := c.CheckServiceTicket(context.Background(), serviceTicketValid)
	require.NoError(t, err)
	require.Equal(t, 42, int(ticketS.DstID))

	ticketS, err = c.CheckServiceTicket(context.Background(), serviceTicketValidAdditional)
	require.NoError(t, err)
	require.Equal(t, 43, int(ticketS.DstID))

	_, err = c.CheckServiceTicket(context.Background(), serviceTicketInvalid)
	require.Error(t, err)
}

func TestDisableDstCheck(t *testing.T) {
	c, err := tvmauth.NewToolClient(disableDstCheckOptions(t), &nop.Logger{})
	require.NoError(t, err)
	defer c.Destroy()

	ticketS, err := c.CheckServiceTicket(context.Background(), serviceTicketInvalid)
	require.NoError(t, err)
	require.Equal(t, 100502, int(ticketS.DstID))
}
