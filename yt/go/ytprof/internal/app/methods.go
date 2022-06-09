package app

import (
	"context"
	"time"

	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc"

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
	return storage.Metaquery{
		Query:       am.Query,
		QueryLimit:  int(a.config.QueryLimit),
		Period:      period,
		ResultSkip:  int(am.ResultSkip),
		ResultLimit: int(am.ResultLimit),
	}, err
}

func (a *App) storageMatadataToAPI(sm ytprof.ProfileMetadata) (*api.Metadata, error) {
	am := &api.Metadata{
		ProfileType: sm.Metadata.MapData["ProfileType"],
		Host:        sm.Metadata.MapData["Host"],
		ArcRevision: sm.Metadata.MapData["ArcRevision"],
		ProfileID:   ytprof.GUIDFormProfID(sm.ProfID()).String(),
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

	res := make([]*api.Metadata, len(resp))

	for id, metadata := range resp {
		res[id], err = a.storageMatadataToAPI(metadata)
		if err != nil {
			a.l.Error("metadata convertion failed", log.Error(err))
			return nil, err
		}
	}

	a.l.Error("list request succeded", log.Int("profiles found", len(res)))

	return &api.ListResponse{Metadata: res}, nil
}

func (a *App) Get(ctx context.Context, in *api.GetRequest, opts ...grpc.CallOption) (*httpbody.HttpBody, error) {
	profileGUID, err := guid.ParseString(in.ProfileID)
	if err != nil {
		a.l.Error("parsing guid failed", log.Error(err), log.String("guid", in.ProfileID))
		return nil, err
	}

	resp, err := a.ts.FindData(ctx, ytprof.ProfIDFromGUID(profileGUID))
	if err != nil {
		a.l.Error("metaquery failed", log.Error(err))
		return nil, err
	}

	a.l.Error("get request succeded", log.String("ProfileID", in.ProfileID))

	return &httpbody.HttpBody{
		ContentType: "application/pprof",
		Data:        resp.Data,
	}, nil
}
