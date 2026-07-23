package tvmtool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/slices"
	"go.ytsaurus.tech/library/go/yandex/tvm"
	"go.ytsaurus.tech/library/go/yandex/tvm/tvmtool/internal/cache"
)

const (
	dialTimeout    = 100 * time.Millisecond
	requestTimeout = 500 * time.Millisecond
	keepAlive      = 60 * time.Second
	bgRefreshFreq  = 10 * time.Minute
	// cacheTTL can't be lower than a few hours: https://st.yandex-team.ru/PASSPINFRA-3288#671a87b6bef21e1ffe538c9e
	cacheTTL    = 2 * time.Hour
	cacheMaxTTL = 11 * time.Hour
)

var (
	ErrNoServiceOrUserTicket = xerrors.New("tvmtool: service ticket or user ticket is required")
	ErrRequiredSrcV2         = xerrors.New("tvm api v2 requires src alias")
)

var (
	_ tvm.Client             = (*Client)(nil)
	_ tvm.ClientV2           = (*Client)(nil)
	_ tvm.ClientMultiVersion = (*Client)(nil)
)

type (
	Client struct {
		lastSync        int64
		apiURI          string
		baseURI         string
		src             string
		authToken       string
		bbEnv           string
		refreshFreq     int64
		bgCtx           context.Context
		bgCancel        context.CancelFunc
		inFlightRefresh uint32
		cache           *cache.Cache
		pingRequest     *http.Request
		ownHTTPClient   bool
		httpClient      *http.Client
		l               log.Structured
		cachedRoles     *atomic.Pointer[tvm.Roles]
		inFlightChecks  *singleflight.Group
	}

	uid tvm.UID

	ticketsInfo struct {
		born    time.Time
		tickets ticketsResponse
	}

	errResponse struct {
		Status string `json:"status"`
		ErrMsg string `json:"error"`
	}

	ticketsResponse map[string]struct {
		Error  string       `json:"error"`
		Ticket string       `json:"ticket"`
		TvmID  tvm.ClientID `json:"tvm_id"`
	}

	checkV2Response struct {
		Status  string              `json:"status"`
		Error   string              `json:"error"`
		Service checkSrvV2Response  `json:"service"`
		User    checkUserV2Response `json:"user"`
	}

	checkSrvV2Response struct {
		SrcID   tvm.ClientID    `json:"src"`
		Error   string          `json:"error"`
		DbgInfo string          `json:"debug_string"`
		LogInfo string          `json:"logging_string"`
		Roles   json.RawMessage `json:"roles"`
	}

	checkUserV2Response struct {
		DefaultUID uid             `json:"default_uid"`
		UIDs       []uid           `json:"uids"`
		Scopes     []string        `json:"scopes"`
		Error      string          `json:"error"`
		DbgInfo    string          `json:"debug_string"`
		Env        tvm.BlackboxEnv `json:"env"`
		LogInfo    string          `json:"logging_string"`
		Location   tvm.Location    `json:"location"`
		DeviceType tvm.DeviceType  `json:"device_type"`
		Roles      json.RawMessage `json:"roles"`
	}

	checkSrvResponse struct {
		SrcID     tvm.ClientID `json:"src"`
		DstID     tvm.ClientID `json:"dst"`
		IssuerUID *tvm.UID     `json:"issuer_uid"`
		Error     string       `json:"error"`
		DbgInfo   string       `json:"debug_string"`
		LogInfo   string       `json:"logging_string"`
	}

	checkUserResponse struct {
		DefaultUID tvm.UID         `json:"default_uid"`
		UIDs       []tvm.UID       `json:"uids"`
		Scopes     []string        `json:"scopes"`
		Error      string          `json:"error"`
		DbgInfo    string          `json:"debug_string"`
		LogInfo    string          `json:"logging_string"`
		Env        tvm.BlackboxEnv `json:"env"`
		LoginID    string          `json:"login_id"`
		Location   tvm.Location    `json:"location"`
		DeviceType tvm.DeviceType  `json:"device_type"`
	}
)

// NewClient method creates a new tvmtool client.
// You must reuse it to prevent connection/goroutines leakage.
func NewClient(apiURI string, opts ...Option) (*Client, error) {
	apiURI = strings.TrimRight(apiURI, "/")
	baseURI := apiURI + "/tvm"
	pingRequest, err := http.NewRequest("GET", baseURI+"/ping", nil)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: failed to configure client: %w", err)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: keepAlive,
	}).DialContext

	tool := &Client{
		apiURI:        apiURI,
		baseURI:       baseURI,
		refreshFreq:   int64(bgRefreshFreq.Seconds()),
		cache:         cache.New(cacheTTL, cacheMaxTTL),
		pingRequest:   pingRequest,
		l:             &nop.Logger{},
		ownHTTPClient: true,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		cachedRoles:    &atomic.Pointer[tvm.Roles]{},
		inFlightChecks: &singleflight.Group{},
	}

	for _, opt := range opts {
		if err := opt(tool); err != nil {
			return nil, xerrors.Errorf("tvmtool: failed to configure client: %w", err)
		}
	}
	if tool.bgCtx != nil {
		go tool.serviceTicketsRefreshLoop()
	}

	return tool, nil
}

// GetServiceTicketForAlias returns TVM service ticket for alias (via TVM API v1)
//
// WARNING: alias must be configured in tvmtool
//
// TVMTool documentation: https://docs.yandex-team.ru/tvm/pages/tvmtool/usage/v1_api_spec#get-/tvm/tickets
func (c *Client) GetServiceTicketForAlias(ctx context.Context, alias string) (string, error) {
	var (
		cachedTicket string
		cacheStatus  = cache.Miss
	)

	if c.cache != nil {
		c.refreshServiceTickets()

		if cachedTicket, cacheStatus = c.cache.LoadByAlias(alias); cacheStatus == cache.Hit {
			return cachedTicket, nil
		}
	}

	ti, err := c.getServiceTickets(ctx, alias)
	if err != nil {
		if len(cachedTicket) > 0 && cacheStatus == cache.GonnaMissy {
			return cachedTicket, nil
		}
		return "", err
	}

	entry, ok := ti.tickets[alias]
	if !ok {
		return "", xerrors.Errorf("tvmtool: alias %q was not found in response", alias)
	}

	if entry.Error != "" {
		return "", &Error{Code: ErrorOther, Msg: entry.Error}
	}

	ticket := entry.Ticket
	if c.cache != nil {
		c.cache.Store(entry.TvmID, alias, cache.Entry{
			Value: ticket,
			Born:  ti.born,
		})
	}
	return ticket, nil
}

// GetServiceTicket returns TVM service ticket for alias (via TVM API v2)
//
// TVMTool documentation: https://docs.yandex-team.ru/tvm/pages/tvmtool/usage/api_spec#get-/v2/tickets
func (c *Client) GetServiceTicket(ctx context.Context, dstAlias string, opts ...tvm.GetServiceTicketV2Option) (string, error) {
	if c.src == "" {
		return "", ErrRequiredSrcV2
	}
	var (
		options      tvm.GetServiceTicketV2Options
		cachedTicket string
		cacheStatus  = cache.Miss
	)
	for _, opt := range opts {
		opt(&options)
	}

	if options.Src == "" && c.cache != nil {
		c.refreshServiceTickets()

		if cachedTicket, cacheStatus = c.cache.LoadByAlias(dstAlias); cacheStatus == cache.Hit {
			return cachedTicket, nil
		}
	}
	srcAlias := options.Src
	if srcAlias == "" {
		srcAlias = c.src
	}
	ti, err := c.getServiceTicketsV2(ctx, srcAlias, dstAlias)
	if err != nil {
		if len(cachedTicket) > 0 && cacheStatus == cache.GonnaMissy {
			return cachedTicket, nil
		}
		return "", err
	}

	entry, ok := ti.tickets[dstAlias]
	if !ok {
		return "", xerrors.Errorf("tvmtool: alias %q was not found in response", dstAlias)
	}

	if entry.Error != "" {
		return "", &Error{Code: ErrorOther, Msg: entry.Error}
	}

	ticket := entry.Ticket
	if options.Src == "" && c.cache != nil {
		c.cache.Store(entry.TvmID, dstAlias, cache.Entry{
			Value: ticket,
			Born:  ti.born,
		})
	}
	return ticket, nil
}

// CheckTicket checks the user and service tickets (via TVM API v2)
//
// TVMTool documentation: https://docs.yandex-team.ru/tvm/pages/tvmtool/usage/api_spec#get-/v2/check
func (c *Client) CheckTicket(ctx context.Context, serviceTicket, userTicket string, opts ...tvm.CheckTicketV2Option) (*tvm.CheckedTickets, error) {
	if c.src == "" {
		return nil, ErrRequiredSrcV2
	}
	options := tvm.CheckTicketV2Options{Params: url.Values{
		"self":       {c.src},
		"show_roles": {"no"},
	}}
	for _, opt := range opts {
		opt(options)
	}
	params := options.Params
	if serviceTicket == "" && userTicket == "" {
		return nil, ErrNoServiceOrUserTicket
	}

	uniqueFuncId := serviceTicket + "|" + userTicket + "|" + params.Encode()
	v, err, _ := c.inFlightChecks.Do(
		uniqueFuncId,
		func() (any, error) { return c.checkTicket(ctx, serviceTicket, userTicket, params) },
	)
	return v.(*tvm.CheckedTickets), err
}

func (c *Client) checkTicket(ctx context.Context, serviceTicket, userTicket string, params url.Values) (*tvm.CheckedTickets, error) {
	req, err := c.buildRequest(c.apiURI+"/v2/check", params)
	if err != nil {
		return nil, err
	}

	if serviceTicket != "" {
		req.Header.Set("X-Ya-Service-Ticket", serviceTicket)
	}
	if userTicket != "" {
		req.Header.Set("X-Ya-User-Ticket", userTicket)
	}

	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	var respModel checkV2Response
	if err = readResponse(resp, &respModel); err != nil {
		return nil, err
	}

	var errsMsgs []string
	if respModel.Error != "" {
		errsMsgs = append(errsMsgs, respModel.Error)
	}

	ticketInfo := &tvm.CheckedTickets{
		User: tvm.CheckedUserTicket{
			DefaultUID: tvm.UID(respModel.User.DefaultUID),
			UIDs:       slices.Map(respModel.User.UIDs, func(tvmID uid) tvm.UID { return tvm.UID(tvmID) }),
			Env:        respModel.User.Env,
			Scopes:     respModel.User.Scopes,
			DbgInfo:    respModel.User.DbgInfo,
			LogInfo:    respModel.User.LogInfo,
			Location:   respModel.User.Location,
			DeviceType: respModel.User.DeviceType,
		},
		Service: tvm.CheckedServiceTicket{
			SrcID:   respModel.Service.SrcID,
			DbgInfo: respModel.Service.DbgInfo,
			LogInfo: respModel.Service.LogInfo,
		},
	}

	if len(errsMsgs) > 0 {
		return ticketInfo, &TicketError{Status: TicketErrorOther, Msg: strings.Join(errsMsgs, "; ")}
	}
	return ticketInfo, nil
}

// GetServiceTicketForID returns TVM service ticket for destination application id (via TVM API v1)
//
// WARNING: id must be configured in tvmtool
//
// TVMTool documentation: https://docs.yandex-team.ru/tvm/pages/tvmtool/usage/v1_api_spec#get-/tvm/tickets
func (c *Client) GetServiceTicketForID(ctx context.Context, dstID tvm.ClientID) (string, error) {
	var (
		cachedTicket string
		cacheStatus  = cache.Miss
	)

	if c.cache != nil {
		c.refreshServiceTickets()

		if cachedTicket, cacheStatus = c.cache.Load(dstID); cacheStatus == cache.Hit {
			return cachedTicket, nil
		}
	}

	alias := strconv.FormatUint(uint64(dstID), 10)
	ti, err := c.getServiceTickets(ctx, alias)
	if err != nil {
		if len(cachedTicket) > 0 && cacheStatus == cache.GonnaMissy {
			return cachedTicket, nil
		}
		return "", err
	}

	entry, ok := ti.tickets[alias]
	if !ok {
		// ok, let's find him
		for candidateAlias, candidate := range ti.tickets {
			if candidate.TvmID == dstID {
				entry = candidate
				alias = candidateAlias
				ok = true
				break
			}
		}

		if !ok {
			return "", xerrors.Errorf("tvmtool: dst %q was not found in response", alias)
		}
	}

	if entry.Error != "" {
		return "", &Error{Code: ErrorOther, Msg: entry.Error}
	}

	ticket := entry.Ticket
	if c.cache != nil {
		c.cache.Store(dstID, alias, cache.Entry{
			Value: ticket,
			Born:  ti.born,
		})
	}
	return ticket, nil
}

// Close stops background ticket updates (if configured) and closes idle connections.
func (c *Client) Close() {
	if c.bgCancel != nil {
		c.bgCancel()
	}

	if c.ownHTTPClient {
		c.httpClient.CloseIdleConnections()
	}
}

func (c *Client) refreshServiceTickets() {
	if c.bgCtx != nil {
		// service tickets will be updated at background in the separated goroutine
		return
	}

	now := time.Now().Unix()
	if now-atomic.LoadInt64(&c.lastSync) > c.refreshFreq {
		atomic.StoreInt64(&c.lastSync, now)
		if atomic.CompareAndSwapUint32(&c.inFlightRefresh, 0, 1) {
			go c.doServiceTicketsRefresh(context.Background())
		}
	}
}

func (c *Client) serviceTicketsRefreshLoop() {
	var ticker = time.NewTicker(time.Duration(c.refreshFreq) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.bgCtx.Done():
			return
		case <-ticker.C:
			c.doServiceTicketsRefresh(c.bgCtx)
		}
	}
}

func (c *Client) doServiceTicketsRefresh(ctx context.Context) {
	defer atomic.CompareAndSwapUint32(&c.inFlightRefresh, 1, 0)

	c.cache.Gc()
	aliases := c.cache.Aliases()
	if len(aliases) == 0 {
		return
	}

	c.l.Debug("tvmtool: service ticket update started")
	defer c.l.Debug("tvmtool: service ticket update finished")

	// fast path: batch update, must work most of time
	err := c.refreshServiceTicket(ctx, aliases...)
	if err == nil {
		return
	}

	if tvmErr, ok := err.(*Error); ok && tvmErr.Code != ErrorBadRequest {
		c.l.Error(
			"tvmtool: failed to refresh all service tickets at background",
			log.Strings("dsts", aliases),
			log.Error(err),
		)

		// if we have non "bad request" error - something really terrible happens, nothing to do with it :(
		// TODO(buglloc): implement adaptive refreshFreq based on errors?
		return
	}

	// slow path: trying to update service tickets one by one
	c.l.Error(
		"tvmtool: failed to refresh all service tickets at background, switched to slow path",
		log.Strings("dsts", aliases),
		log.Error(err),
	)

	for _, dst := range aliases {
		if err := c.refreshServiceTicket(ctx, dst); err != nil {
			c.l.Error(
				"tvmtool: failed to refresh service ticket at background",
				log.String("dst", dst),
				log.Error(err),
			)
		}
	}
}

func (c *Client) refreshServiceTicket(ctx context.Context, dsts ...string) error {
	ti, err := c.getServiceTickets(ctx, strings.Join(dsts, ","))
	if err != nil {
		return err
	}

	for _, dst := range dsts {
		entry, ok := ti.tickets[dst]
		if !ok {
			c.l.Error(
				"tvmtool: destination was not found in tvmtool response",
				log.String("dst", dst),
			)
			continue
		}

		if entry.Error != "" {
			c.l.Error(
				"tvmtool: failed to get service ticket for destination",
				log.String("dst", dst),
				log.String("err", entry.Error),
			)
			continue
		}

		c.cache.Store(entry.TvmID, dst, cache.Entry{
			Value: entry.Ticket,
			Born:  ti.born,
		})
	}

	return nil
}

func (c *Client) getServiceTickets(ctx context.Context, dst string) (*ticketsInfo, error) {
	params := url.Values{
		"dsts": {dst},
	}

	if c.src != "" {
		params.Set("src", c.src)
	}

	req, err := c.buildRequest(c.baseURI+"/tickets", params)
	if err != nil {
		return nil, err
	}

	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	var result ticketsInfo
	err = readResponse(resp, &result.tickets)
	if err != nil {
		return nil, err
	}

	result.born = c.parseBornDate(resp)
	return &result, nil
}

func (c *Client) getServiceTicketsV2(ctx context.Context, src, dst string) (*ticketsInfo, error) {
	params := url.Values{
		"dsts": {dst},
		"self": {src},
	}

	req, err := c.buildRequest(c.apiURI+"/v2/tickets", params)
	if err != nil {
		return nil, err
	}

	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	var result ticketsInfo
	err = readResponse(resp, &result.tickets)
	if err != nil {
		return nil, err
	}

	result.born = c.parseBornDate(resp)
	return &result, nil
}

// Check TVM service ticket (via TVM API v1)
//
// TVMTool documentation: https://docs.yandex-team.ru/tvm/pages/tvmtool/usage/v1_api_spec#get-/tvm/checksrv
func (c *Client) CheckServiceTicket(ctx context.Context, ticket string) (*tvm.CheckedServiceTicket, error) {
	params := url.Values{}
	if c.src != "" {
		params.Set("dst", c.src)
	}
	uniqueFuncId := "service:" + ticket + "|" + params.Encode()
	v, err, _ := c.inFlightChecks.Do(
		uniqueFuncId,
		func() (any, error) { return c.checkServiceTicket(ctx, ticket, params) },
	)
	return v.(*tvm.CheckedServiceTicket), err
}

func (c *Client) checkServiceTicket(ctx context.Context, ticket string, params url.Values) (*tvm.CheckedServiceTicket, error) {

	req, err := c.buildRequest(c.baseURI+"/checksrv", params)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Ya-Service-Ticket", ticket)
	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	var result checkSrvResponse
	if err = readResponse(resp, &result); err != nil {
		return nil, err
	}

	ticketInfo := &tvm.CheckedServiceTicket{
		SrcID:   result.SrcID,
		DstID:   result.DstID,
		DbgInfo: result.DbgInfo,
		LogInfo: result.LogInfo,
	}
	if result.IssuerUID != nil {
		ticketInfo.IssuerUID = *result.IssuerUID
	}

	if resp.StatusCode == http.StatusForbidden {
		err = &TicketError{Status: TicketErrorOther, Msg: result.Error}
	}

	return ticketInfo, err
}

// Check TVM user ticket (via TVM API v1)
//
// TVMTool documentation: https://docs.yandex-team.ru/tvm/pages/tvmtool/usage/v1_api_spec#get-/tvm/checkusr
func (c *Client) CheckUserTicket(ctx context.Context, ticket string, opts ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error) {
	options := &tvm.CheckUserTicketOptions{
		EnvOverride: nil,
	}
	for _, opt := range opts {
		opt(options)
	}

	params := url.Values{}
	if options.EnvOverride != nil {
		params.Set("override_env", options.EnvOverride.String())
	} else if c.bbEnv != "" {
		params.Set("override_env", c.bbEnv)
	}
	uniqueFuncId := "user:" + ticket + "|" + params.Encode()
	v, err, _ := c.inFlightChecks.Do(
		uniqueFuncId,
		func() (any, error) { return c.checkUserTicket(ctx, ticket, params) },
	)
	return v.(*tvm.CheckedUserTicket), err
}

func (c *Client) checkUserTicket(ctx context.Context, ticket string, params url.Values) (*tvm.CheckedUserTicket, error) {

	req, err := c.buildRequest(c.baseURI+"/checkusr", params)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Ya-User-Ticket", ticket)
	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	var result checkUserResponse
	if err = readResponse(resp, &result); err != nil {
		return nil, err
	}

	ticketInfo := &tvm.CheckedUserTicket{
		DefaultUID: result.DefaultUID,
		UIDs:       result.UIDs,
		Env:        result.Env,
		Scopes:     result.Scopes,
		DbgInfo:    result.DbgInfo,
		LogInfo:    result.LogInfo,
		LoginID:    result.LoginID,
		Location:   result.Location,
		DeviceType: result.DeviceType,
	}

	if resp.StatusCode == http.StatusForbidden {
		err = &TicketError{Status: TicketErrorOther, Msg: result.Error}
	}

	return ticketInfo, err
}

// Checks TVMTool liveness (via TVM API v1)
//
// TVMTool documentation: https://docs.yandex-team.ru/tvm/pages/tvmtool/usage/v1_api_spec#get-/tvm/ping
func (c *Client) GetStatus(ctx context.Context) (tvm.ClientStatusInfo, error) {
	resp, err := c.doRequest(ctx, c.pingRequest)
	if err != nil {
		return tvm.ClientStatusInfo{Status: tvm.ClientError},
			&PingError{Code: PingCodeDie, Err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return tvm.ClientStatusInfo{Status: tvm.ClientError},
			&PingError{Code: PingCodeDie, Err: err}
	}

	var status tvm.ClientStatusInfo
	switch resp.StatusCode {
	case http.StatusOK:
		// OK!
		status = tvm.ClientStatusInfo{Status: tvm.ClientOK}
		err = nil
	case http.StatusPartialContent:
		status = tvm.ClientStatusInfo{Status: tvm.ClientWarning}
		err = &PingError{Code: PingCodeWarning, Err: xerrors.New(string(body))}
	case http.StatusInternalServerError:
		status = tvm.ClientStatusInfo{Status: tvm.ClientError}
		err = &PingError{Code: PingCodeError, Err: xerrors.New(string(body))}
	default:
		status = tvm.ClientStatusInfo{Status: tvm.ClientError}
		err = &PingError{Code: PingCodeOther, Err: xerrors.Errorf("tvmtool: unexpected status: %d", resp.StatusCode)}
	}
	return status, err
}

// Returns TVMTool version
func (c *Client) Version(ctx context.Context) (string, error) {
	resp, err := c.doRequest(ctx, c.pingRequest)
	if err != nil {
		return "", err
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	return resp.Header.Get("Server"), nil
}

// GetRoles get roles from IDM to all possible consumers (via TVM API v2)
//
// TVMTool documentation: https://docs.yandex-team.ru/tvm/pages/tvmtool/usage/api_spec#get-/v2/roles
func (c *Client) GetRoles(ctx context.Context) (*tvm.Roles, error) {
	var cachedRevision string
	cachedRolesValue := c.cachedRoles.Load()
	if cachedRolesValue != nil {
		cachedRevision = cachedRolesValue.GetMeta().Revision
	}

	params := url.Values{
		"self": []string{c.src},
	}
	req, err := c.buildRequest(c.apiURI+"/v2/roles", params)
	if err != nil {
		return nil, err
	}

	if cachedRevision != "" {
		req.Header.Set("If-None-Match", "\""+cachedRevision+"\"")
	}

	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusNotModified {
		if cachedRolesValue == nil {
			return nil, xerrors.Errorf("tvmtool: logic error got 304 on empty cached roles data")
		}
		return cachedRolesValue, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, xerrors.Errorf("tvmtool: getroles: [%d] %s", resp.StatusCode, resp.Status)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: getroles: [%d] %s: %w", resp.StatusCode, resp.Status, err)
	}

	roles, err := tvm.NewRoles(b)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: unable to parse roles: %w", err)
	}

	c.cachedRoles.Store(roles)

	return roles, nil
}

func (c *Client) buildRequest(url string, params url.Values) (*http.Request, error) {
	req, err := http.NewRequest("GET", url+"?"+params.Encode(), nil)
	if err != nil {
		return nil, &Error{
			Code:       ErrorOther,
			Msg:        "build request",
			Retriable:  false,
			Underlying: err,
		}
	}

	req.Header.Set("Authorization", c.authToken)
	return req, nil
}

func (c *Client) doRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	req = req.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err == nil {
		return resp, nil
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Since tvmtool lives on loopback, almost any transport error should be retried.
	// For example tvmtool may not yet be running or be overloaded.

	return nil, &Error{
		Code:       ErrorOther,
		Msg:        "call tvmtool",
		Retriable:  true,
		Underlying: err,
	}
}

func (c *Client) parseBornDate(resp *http.Response) time.Time {
	bornStr := resp.Header.Get("X-Ya-Tvmtool-Data-Birthtime")
	if len(bornStr) == 0 {
		return time.Now()
	}

	born, err := strconv.ParseInt(strings.TrimSpace(bornStr), 10, 64)
	if err != nil {
		c.l.Warn("tvmtool returns invalid 'X-Ya-Tvmtool-Data-Birthtime' header, use now() instead",
			log.String("value", bornStr),
			log.Error(err),
		)

		return time.Now()
	}

	return time.Unix(born, 0)
}

func readResponse(resp *http.Response, dst interface{}) error {
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return &Error{
			Code:       ErrorOther,
			Msg:        "read tvmtool response",
			Retriable:  true,
			Underlying: err,
		}
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusForbidden:
		// ok
		return json.Unmarshal(body, dst)
	case http.StatusUnauthorized:
		return &Error{
			Code: ErrorAuthFail,
			Msg:  string(body),
		}
	case http.StatusBadRequest:
		var errResp errResponse
		jsonErr := json.Unmarshal(body, &errResp)
		if jsonErr == nil {
			return &Error{
				Code: ErrorBadRequest,
				Msg:  errResp.ErrMsg,
			}
		}
		return &Error{
			Code: ErrorBadRequest,
			Msg:  string(body),
		}
	case http.StatusInternalServerError:
		return &Error{
			Code:      ErrorOther,
			Msg:       string(body),
			Retriable: true,
		}
	default:
		return &Error{
			Code: ErrorOther,
			Msg:  fmt.Sprintf("tvmtool: unexpected status: %d, msg: %s", resp.StatusCode, string(body)),
		}
	}
}

func (tvmID *uid) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return err
	}
	*tvmID = uid(id)
	return nil
}
