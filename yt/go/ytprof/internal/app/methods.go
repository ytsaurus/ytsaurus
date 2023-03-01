package app

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/pprof/driver"
	"github.com/google/pprof/profile"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/api"
	"a.yandex-team.ru/yt/go/ytprof/internal/storage"
)

var errStorageNotFound = fmt.Errorf("storage not found")

func apiTimePeriodToStorage(at *api.TimePeriod) (storage.TimestampPeriod, error) {
	tmin, err := yson.UnmarshalTime(at.PeriodStartTime)
	if err != nil {
		return storage.TimestampPeriod{}, err
	}

	tmax, err := yson.UnmarshalTime(at.PeriodEndTime)

	return storage.TimestampPeriod{Start: time.Time(tmin), End: time.Time(tmax)}, err
}

func (a *App) apiMetaqueryToStorage(am *api.Metaquery) (storage.Metaquery, error) {
	period, err := apiTimePeriodToStorage(am.TimePeriod)
	metaquery := storage.Metaquery{
		Query:            am.Query,
		QueryLimit:       int(a.config.QueryLimit),
		Period:           period,
		ResultSkip:       int(am.ResultSkip),
		ResultLimit:      int(am.ResultLimit),
		MetadataPatterns: am.MetadataPattern.UserTags,
	}
	if metaquery.MetadataPatterns == nil {
		metaquery.MetadataPatterns = map[string]string{}
	}
	if metaquery.Query == "" {
		metaquery.Query = "true"
	}
	if len(am.MetadataPattern.ProfileType) > 0 {
		metaquery.MetadataPatterns["ProfileType"] = am.MetadataPattern.ProfileType
	}
	if len(am.MetadataPattern.Host) > 0 {
		metaquery.MetadataPatterns["Host"] = am.MetadataPattern.Host
	}
	if len(am.MetadataPattern.ArcRevision) > 0 {
		metaquery.MetadataPatterns["ArcRevision"] = am.MetadataPattern.ArcRevision
	}

	return metaquery, err
}

func (a *App) storageMetadataToAPI(sm ytprof.ProfileMetadata) (*api.Metadata, error) {
	am := &api.Metadata{
		ProfileType: sm.Metadata.MapData["ProfileType"],
		Host:        sm.Metadata.MapData["Host"],
		ArcRevision: sm.Metadata.MapData["ArcRevision"],
		ProfileId:   ytprof.GUIDFormProfID(sm.ProfID()).String(),
		UserTags:    sm.Metadata.MapData,
	}

	delete(am.UserTags, "ProfileType")
	delete(am.UserTags, "Host")
	delete(am.UserTags, "ArcRevision")

	var err error
	am.Timestamp, err = yson.MarshalTime(yson.Time(sm.Timestamp.Time()))

	return am, err
}

func (a *App) List(ctx context.Context, in *api.ListRequest, opts ...grpc.CallOption) (*api.ListResponse, error) {
	tsc, ok := a.TableStorage(in.Metaquery.System)
	if !ok {
		a.l.Error("table storage not found", log.String("system", in.Metaquery.System))
		return nil, errStorageNotFound
	}

	metaquery, err := a.apiMetaqueryToStorage(in.Metaquery)
	if err != nil {
		a.l.Error("time conversion failed", log.Error(err))
		return nil, err
	}

	resp, err := tsc.MetadataQueryExpr(ctx, metaquery, false)
	if err != nil {
		a.l.Error("metaquery failed", log.Error(err))
		return nil, err
	}

	// TODO: if this is too slow add cash for sizes
	metaquery.ResultSkip = 0
	metaquery.ResultLimit = 0
	respLen, err := tsc.MetadataIDsQueryExpr(ctx, metaquery, false)
	if err != nil {
		a.l.Error("metaquery failed", log.Error(err))
		return nil, err
	}

	res := make([]*api.Metadata, len(resp))

	for id, metadata := range resp {
		res[id], err = a.storageMetadataToAPI(metadata)
		if err != nil {
			a.l.Error("metadata conversion failed", log.Error(err))
			return nil, err
		}
	}

	a.l.Debug("list request succeeded", log.Int("profiles_found", len(res)))

	return &api.ListResponse{Metadata: res, Size: int32(len(respLen))}, nil
}

func (a *App) Get(ctx context.Context, in *api.GetRequest, opts ...grpc.CallOption) (*httpbody.HttpBody, error) {
	tsc, ok := a.TableStorage(in.System)
	if !ok {
		a.l.Error("table storage not found", log.String("system", in.System))
		return nil, errStorageNotFound
	}

	profileGUID, err := guid.ParseString(in.ProfileId)
	if err != nil {
		a.l.Error("parsing guid failed", log.Error(err), log.String("guid", in.ProfileId))
		return nil, err
	}

	resp, err := tsc.FindData(ctx, ytprof.ProfIDFromGUID(profileGUID))
	if err != nil {
		a.l.Error("finding profile failed: profile with such id does not exist", log.Error(err))
		return nil, err
	}

	a.l.Debug("get request succeeded", log.String("ProfileID", in.ProfileId))

	return &httpbody.HttpBody{
		ContentType: "application/pprof",
		Data:        resp.Data,
	}, nil
}

func (a *App) Merge(ctx context.Context, in *api.MergeRequest, opts ...grpc.CallOption) (*httpbody.HttpBody, error) {
	tsc, ok := a.TableStorage(in.System)
	if !ok {
		a.l.Error("table storage not found", log.String("system", in.System))
		return nil, errStorageNotFound
	}

	profileGUIDs := make([]ytprof.ProfID, len(in.ProfileIds))

	for i, profileString := range in.ProfileIds {
		profileGUID, err := guid.ParseString(profileString)
		if err != nil {
			a.l.Error("parsing guid failed", log.Error(err), log.String("guid", profileString))
			return nil, err
		}
		profileGUIDs[i] = ytprof.ProfIDFromGUID(profileGUID)
	}

	profile, err := tsc.FindAndMergeProfiles(ctx, profileGUIDs)
	if err != nil {
		a.l.Error("find and merge failed", log.Error(err))
		return nil, err
	}

	var buf bytes.Buffer
	err = profile.Write(&buf)
	if err != nil {
		a.l.Error("writing to buffer failed", log.Error(err))
		return nil, err
	}

	a.l.Debug("merge request succeeded", log.Int("total", len(profileGUIDs)))

	return &httpbody.HttpBody{
		ContentType: "application/pprof",
		Data:        buf.Bytes(),
	}, nil
}

func (a *App) MergeLink(ctx context.Context, in *api.MergeRequest, opts ...grpc.CallOption) (*api.MergeLinkResponse, error) {
	tsc, ok := a.TableStorage(in.System)
	if !ok {
		a.l.Error("table storage not found", log.String("system", in.System))
		return nil, errStorageNotFound
	}

	profileGUIDs := make([]ytprof.ProfID, len(in.ProfileIds))

	for i, profileString := range in.ProfileIds {
		profileGUID, err := guid.ParseString(profileString)
		if err != nil {
			a.l.Error("parsing guid failed", log.Error(err), log.String("guid", profileString))
			return nil, err
		}
		profileGUIDs[i] = ytprof.ProfIDFromGUID(profileGUID)
	}

	profileMerged, err := tsc.FindAndMergeProfiles(ctx, profileGUIDs)
	if err != nil {
		a.l.Error("find and merge failed", log.Error(err))
		return nil, err
	}

	tsm, ok := a.TableStorage(ManualSystem)
	if !ok {
		a.l.Error("table storage not found", log.String("system", ManualSystem))
		return nil, errStorageNotFound
	}

	guids, err := tsm.PushData(
		ctx,
		[]*profile.Profile{profileMerged},
		[]string{"merged"},
		"",
		"",
		"",
		map[string]string{},
	)
	if err != nil {
		a.l.Error("storing merged profile failed", log.Error(err))
		return nil, err
	}

	a.l.Debug("merge request succeeded", log.Int("total", len(profileGUIDs)))

	return &api.MergeLinkResponse{
		Link: GetProfileLink(UIManualRequestPrefix, guids[0]),
	}, nil
}

func (a *App) MergeAll(ctx context.Context, in *api.MergeAllRequest, opts ...grpc.CallOption) (*httpbody.HttpBody, error) {
	tsc, ok := a.TableStorage(in.Metaquery.System)
	if !ok {
		a.l.Error("table storage not found", log.String("system", in.Metaquery.System))
		return nil, errStorageNotFound
	}

	metaquery, err := a.apiMetaqueryToStorage(in.Metaquery)
	if err != nil {
		a.l.Error("time conversion failed", log.Error(err))
		return nil, err
	}

	metaquery.ResultSkip = 0
	metaquery.ResultLimit = 0

	resp, err := tsc.MetadataIDsQueryExpr(ctx, metaquery, false)
	if err != nil {
		a.l.Error("metaquery failed", log.Error(err))
		return nil, err
	}

	a.l.Debug("merge all request started", log.Int("profiles_found", len(resp)))

	profile, err := tsc.FindAndMergeProfiles(ctx, resp)
	if err != nil {
		a.l.Error("find and merge failed", log.Error(err))
		return nil, err
	}

	var buf bytes.Buffer
	err = profile.Write(&buf)
	if err != nil {
		a.l.Error("writing to buffer failed", log.Error(err))
		return nil, err
	}

	a.l.Debug("merge all request succeeded", log.Int("total", len(resp)))

	return &httpbody.HttpBody{
		ContentType: "application/pprof",

		Data: buf.Bytes(),
	}, nil
}

func (a *App) SuggestTags(ctx context.Context, in *api.SuggestTagsRequest, opts ...grpc.CallOption) (*api.SuggestTagsResponse, error) {
	tsc, ok := a.TableStorage(in.System)
	if !ok {
		a.l.Error("table storage not found", log.String("system", in.System))
		return nil, errStorageNotFound
	}

	resp, err := tsc.FindTags(ctx)
	if err != nil {
		a.l.Error("find_tags failed", log.Error(err))
		return nil, err
	}

	a.l.Debug("find_tags request succeeded")

	sort.Strings(resp)

	return &api.SuggestTagsResponse{
		Tag: resp,
	}, nil
}

func (a *App) Systems(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*api.SystemsResponse, error) {
	uiSystems := []string{}

	for _, system := range a.systems {
		if _, ok := NonUISystems[system]; !ok {
			uiSystems = append(uiSystems, system)
		}
	}

	sort.Strings(uiSystems)

	return &api.SystemsResponse{
		System: uiSystems,
	}, nil
}

func (a *App) SuggestValues(ctx context.Context, in *api.SuggestValuesRequest, opts ...grpc.CallOption) (*api.SuggestValuesResponse, error) {
	tsc, ok := a.TableStorage(in.System)
	if !ok {
		a.l.Error("table storage not found", log.String("system", in.System))
		return nil, errStorageNotFound
	}

	resp, err := tsc.FindTagValues(ctx, in.Tag)
	if err != nil {
		a.l.Error("find_tags failed", log.Error(err))
		return nil, err
	}

	a.l.Error("find_values request succeeded")

	sort.Strings(resp)

	return &api.SuggestValuesResponse{
		Value: resp,
	}, nil
}

func (a *App) UIHandler(w http.ResponseWriter, r *http.Request, system string) {
	a.l.Debug("receiving HTTP UI request",
		log.String("path", r.URL.Path),
		log.String("system", system),
	)

	guidString := chi.RouteContext(r.Context()).URLParam("profileID")
	profileGUID, err := guid.ParseString(guidString)
	if err != nil {
		a.l.Error("failed HTTP UI request: wrong guid format", log.Error(err))
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	tsc, ok := a.TableStorage(system)
	if !ok {
		a.l.Error("failed HTTP UI request: table storage not found",
			log.Error(errStorageNotFound),
			log.String("system", system),
		)
		http.Error(w, errStorageNotFound.Error(), http.StatusNotFound)
		return
	}

	profile, err := tsc.FindProfile(r.Context(), ytprof.ProfIDFromGUID(profileGUID))
	if err != nil {
		a.l.Error("failed HTTP UI request: profile not found", log.Error(err))
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	fetcher := profileFetcher{profile: profile}

	uiPrefix := UIRequestPrefix
	if len(system) > 0 {
		uiPrefix = fmt.Sprintf("/%s%s", system, UIRequestPrefix)
	}

	fullPrefix := fmt.Sprintf("%s/%s", uiPrefix, profileGUID)

	strippedURL := strings.TrimPrefix(r.URL.Path, fullPrefix)

	a.l.Debug("finding profile succeeded",
		log.String("profile_id", profileGUID.String()),
		log.String("stripped_url", strippedURL))

	server := func(args *driver.HTTPServerArgs) error {
		handler := http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
			h, ok := args.Handlers[strippedURL]
			if !ok {
				http.NotFound(writer, req)
				return
			}
			h.ServeHTTP(writer, req)
		})

		http.StripPrefix(fullPrefix, handler).(http.HandlerFunc)(w, r)

		return nil
	}

	opts := &driver.Options{
		HTTPServer: server,
		Fetch:      &fetcher,
		Flagset:    baseFlags(),
	}

	err = driver.PProf(opts)
	if err != nil {
		a.l.Error("failed request to pprof UI", log.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.l.Debug("request to pprof UI succeeded")
}
