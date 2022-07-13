package app

import (
	"bytes"
	"context"
	"time"

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
		MatadataPatterns: am.MetadataPattern.UserTags,
	}
	if metaquery.MatadataPatterns == nil {
		metaquery.MatadataPatterns = map[string]string{}
	}
	if metaquery.Query == "" {
		metaquery.Query = "true"
	}
	if len(am.MetadataPattern.ProfileType) > 0 {
		metaquery.MatadataPatterns["ProfileType"] = am.MetadataPattern.ProfileType
	}
	if len(am.MetadataPattern.Host) > 0 {
		metaquery.MatadataPatterns["Host"] = am.MetadataPattern.Host
	}
	if len(am.MetadataPattern.ArcRevision) > 0 {
		metaquery.MatadataPatterns["ArcRevision"] = am.MetadataPattern.ArcRevision
	}

	return metaquery, err
}

func (a *App) storageMatadataToAPI(sm ytprof.ProfileMetadata) (*api.Metadata, error) {
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
	metaquery, err := a.apiMetaqueryToStorage(in.Metaquery)
	if err != nil {
		a.l.Error("time convertion failed", log.Error(err))
		return nil, err
	}

	resp, err := a.ts.MetadataQueryExpr(ctx, metaquery)
	if err != nil {
		a.l.Error("metaquery failed", log.Error(err))
		return nil, err
	}

	// TODO: if this is too slow add cash for sizes
	metaquery.ResultSkip = 0
	metaquery.ResultLimit = 0
	respLen, err := a.ts.MetadataIdsQueryExpr(ctx, metaquery)
	if err != nil {
		a.l.Error("metaquery failed", log.Error(err))
		return nil, err
	}

	res := make([]*api.Metadata, len(resp))

	for id, metadata := range resp {
		res[id], err = a.storageMatadataToAPI(metadata)
		if err != nil {
			a.l.Error("metadata convertion failed", log.Error(err))
			return nil, err
		}
	}

	a.l.Debug("list request succeded", log.Int("profiles found", len(res)))

	return &api.ListResponse{Metadata: res, Size: int32(len(respLen))}, nil
}

func (a *App) Get(ctx context.Context, in *api.GetRequest, opts ...grpc.CallOption) (*httpbody.HttpBody, error) {
	profileGUID, err := guid.ParseString(in.ProfileId)
	if err != nil {
		a.l.Error("parsing guid failed", log.Error(err), log.String("guid", in.ProfileId))
		return nil, err
	}

	resp, err := a.ts.FindData(ctx, ytprof.ProfIDFromGUID(profileGUID))
	if err != nil {
		a.l.Error("metaquery failed", log.Error(err))
		return nil, err
	}

	a.l.Debug("get request succeded", log.String("ProfileID", in.ProfileId))

	return &httpbody.HttpBody{
		ContentType: "application/pprof",
		Data:        resp.Data,
	}, nil
}

func (a *App) Merge(ctx context.Context, in *api.MergeRequest, opts ...grpc.CallOption) (*httpbody.HttpBody, error) {
	profileGUIDs := make([]ytprof.ProfID, len(in.ProfileIds))

	for i, profileString := range in.ProfileIds {
		profileGUID, err := guid.ParseString(profileString)
		if err != nil {
			a.l.Error("parsing guid failed", log.Error(err), log.String("guid", profileString))
			return nil, err
		}
		profileGUIDs[i] = ytprof.ProfIDFromGUID(profileGUID)
	}

	profile, err := a.ts.FindAndMergeProfiles(ctx, profileGUIDs)
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

	a.l.Debug("merge request succeded", log.Int("total", len(profileGUIDs)))

	return &httpbody.HttpBody{
		ContentType: "application/pprof",
		Data:        buf.Bytes(),
	}, nil
}

func (a *App) SuggestTags(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*api.SuggestTagsResponse, error) {
	resp, err := a.ts.FindTags(ctx)
	if err != nil {
		a.l.Error("find_tags failed", log.Error(err))
		return nil, err
	}

	a.l.Error("find_tags request succeded")

	return &api.SuggestTagsResponse{
		Tag: resp,
	}, nil
}

func (a *App) SuggestValues(ctx context.Context, in *api.SuggestValuesRequest, opts ...grpc.CallOption) (*api.SuggestValuesResponse, error) {
	resp, err := a.ts.FindTagValues(ctx, in.Tag)
	if err != nil {
		a.l.Error("find_tags failed", log.Error(err))
		return nil, err
	}

	a.l.Error("find_values request succeded")

	return &api.SuggestValuesResponse{
		Value: resp,
	}, nil
}
