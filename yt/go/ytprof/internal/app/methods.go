package app

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"a.yandex-team.ru/library/go/core/log"
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

func apiMetaqueryToStorage(am *api.Metaquery) (storage.Metaquery, error) {
	period, err := apiTimePeriodToStorage(am.TimePeriod)
	return storage.Metaquery{
		Query:      am.Query,
		QueryLimit: int(am.QueryLimit),
		Period:     period,
	}, err
}

func storageMatadataToAPI(sm ytprof.ProfileMetadata) (*api.Metadata, error) {
	am := &api.Metadata{
		ProfileType: sm.Metadata.MapData["ProfileType"],
		Host:        sm.Metadata.MapData["Host"],
		ArcRevision: sm.Metadata.MapData["ArcRevision"],
		GUID:        ytprof.GUIDFormProfID(sm.ProfID()).String(),
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
	metaquery, err := apiMetaqueryToStorage(in.Metaquery)
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
		res[id], err = storageMatadataToAPI(metadata)
		if err != nil {
			a.l.Error("metadata convertion failed", log.Error(err))
			return nil, err
		}
	}

	return &api.ListResponse{Metadata: res}, nil
}
