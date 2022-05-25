package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/google/pprof/profile"

	"a.yandex-team.ru/library/go/core/log"
	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/internal/expressions"
)

func errRowAlreadyExists(table interface{}, rowKey interface{}) error {
	return fmt.Errorf("row with id(%v) already exists in table(%v)", rowKey, table)
}

func errRowNotFound(table interface{}, rowKey interface{}) error {
	return fmt.Errorf("row with id(%v) not found in table(%v)", rowKey, table)
}

type TimestampPeriod struct {
	Start time.Time
	End   time.Time
}

type Metaquery struct {
	Period     TimestampPeriod
	Query      string
	QueryLimit int
}

type TableStorage struct {
	tableData     ypath.Path
	tableMetadata ypath.Path
	yc            yt.Client

	l *logzap.Logger
}

func (p TimestampPeriod) Timestamps() (tmin schema.Timestamp, tmax schema.Timestamp, err error) {
	tmin, err = schema.NewTimestamp(p.Start)
	if err != nil {
		return
	}

	tmax, err = schema.NewTimestamp(p.End)
	return
}

func NewTableStorage(yc yt.Client, path ypath.Path, l *logzap.Logger) *TableStorage {
	p := new(TableStorage)
	p.yc = yc
	p.tableData = path.Child(ytprof.TableData)
	p.tableMetadata = path.Child(ytprof.TableMetadata)
	p.l = l
	return p
}

func NewTableStorageMigrate(flagTablePath string, yt yt.Client, l *logzap.Logger) (*TableStorage, error) {
	tableYTPath := ypath.Path(flagTablePath)

	err := ytprof.MigrateTables(yt, tableYTPath)

	if err != nil {
		l.Error("migraton failed", log.Error(err), log.String("table_path", tableYTPath.String()))
		return nil, err
	}

	storage := NewTableStorage(yt, tableYTPath, l)

	return storage, nil
}

func GenerateNewUID() ytprof.ProfID {
	return ytprof.ProfID{ProfIDLow: rand.Uint64(), ProfIDHigh: rand.Uint64()}
}

func (m *TableStorage) FormMetadata(profile *profile.Profile, host string, profileType string, cluster string, service string) ytprof.Metadata {
	metadata := ytprof.Metadata{
		MapData: map[string]string{},
	}

	metadata.MapData["ProfileType"] = profileType
	metadata.MapData["Host"] = host
	metadata.MapData["Cluster"] = cluster
	metadata.MapData["Service"] = service

	for _, next := range profile.Comments {
		if strings.HasPrefix(next, "binary_version=") {
			metadata.MapData["BinaryVersion"] = strings.TrimPrefix(next, "binary_version=")
		}
		if strings.HasPrefix(next, "arc_revision=") {
			metadata.MapData["ArcRevision"] = strings.TrimPrefix(next, "arc_revision=")
		}
	}

	return metadata
}

func (m *TableStorage) PushData(ctx context.Context, profiles []*profile.Profile, hosts []string, profileType string, cluster string, service string) error {
	tx, err := m.yc.BeginTabletTx(ctx, nil)
	if err != nil {
		return err
	}

	rowsData := make([]interface{}, 0, len(profiles))
	rowsMetadata := make([]interface{}, 0, len(profiles))

	curTime := time.Now()
	timestamp, err := schema.NewTimestamp(curTime)
	if err != nil {
		return err
	}

	for id, profileData := range profiles {
		var buf bytes.Buffer
		err := profileData.Write(&buf)
		if err != nil {
			return err
		}
		uID := GenerateNewUID()

		rowsData = append(rowsData, ytprof.ProfileData{
			ProfIDHigh: uID.ProfIDHigh,
			ProfIDLow:  uID.ProfIDLow,
			Data:       buf.Bytes(),
		})
		rowsMetadata = append(rowsMetadata, ytprof.ProfileMetadata{
			ProfIDHigh: uID.ProfIDHigh,
			ProfIDLow:  uID.ProfIDLow,
			Timestamp:  timestamp,
			Metadata:   m.FormMetadata(profileData, hosts[id], profileType, cluster, service),
		})
	}

	err = tx.InsertRows(ctx, m.tableData, rowsData, nil)
	if err != nil {
		return err
	}

	err = tx.InsertRows(ctx, m.tableMetadata, rowsMetadata, nil)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (m *TableStorage) MetadataIdsQuery(ctx context.Context, minTime schema.Timestamp, maxTime schema.Timestamp, queryLimit int) ([]ytprof.ProfID, error) {
	r, err := m.yc.SelectRows(ctx, m.queryIDsMetadataPeriod(minTime, maxTime, queryLimit), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	resultIDs := make([]ytprof.ProfID, 0)

	for r.Next() {
		var nextVal ytprof.ProfID
		err = r.Scan(&nextVal)
		if err != nil {
			return nil, err
		}

		resultIDs = append(resultIDs, nextVal)
	}

	return resultIDs, r.Err()
}

func (m *TableStorage) MetadataQuery(ctx context.Context, minTime schema.Timestamp, maxTime schema.Timestamp, queryLimit int) ([]ytprof.ProfileMetadata, error) {
	r, err := m.yc.SelectRows(ctx, m.queryMetadataPeriod(minTime, maxTime, queryLimit), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	results := make([]ytprof.ProfileMetadata, 0)

	for r.Next() {
		var nextVal ytprof.ProfileMetadata
		err = r.Scan(&nextVal)
		if err != nil {
			return nil, err
		}

		results = append(results, nextVal)
	}

	return results, r.Err()
}

func (m *TableStorage) MetadataQueryExpr(ctx context.Context, metaquery Metaquery) ([]ytprof.ProfileMetadata, error) {
	tmin, tmax, err := metaquery.Period.Timestamps()
	if err != nil {
		return nil, err
	}

	r, err := m.yc.SelectRows(ctx, m.queryMetadataPeriod(tmin, tmax, metaquery.QueryLimit), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	results := make([]ytprof.ProfileMetadata, 0)

	expr, err := expressions.NewExpression(metaquery.Query)
	if err != nil {
		return nil, err
	}

	for r.Next() {
		var nextVal ytprof.ProfileMetadata
		err = r.Scan(&nextVal)
		if err != nil {
			return nil, err
		}

		var result bool
		result, err = expr.Evaluate(nextVal.Metadata)
		if err != nil {
			return nil, err
		}
		if result {
			results = append(results, nextVal)
		}
	}

	return results, r.Err()
}

func (m *TableStorage) MetadataIdsQueryExpr(ctx context.Context, metaquery Metaquery) ([]ytprof.ProfID, error) {
	tmin, tmax, err := metaquery.Period.Timestamps()
	if err != nil {
		return nil, err
	}

	r, err := m.yc.SelectRows(ctx, m.queryMetadataPeriod(tmin, tmax, metaquery.QueryLimit), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	resultIDs := make([]ytprof.ProfID, 0)

	expr, err := expressions.NewExpression(metaquery.Query)
	if err != nil {
		return nil, err
	}

	for r.Next() {
		var nextVal ytprof.ProfileMetadata
		err = r.Scan(&nextVal)
		if err != nil {
			return nil, err
		}

		var result bool
		result, err = expr.Evaluate(nextVal.Metadata)
		if err != nil {
			return nil, err
		}
		if result {
			resultIDs = append(resultIDs, nextVal.ProfID())
		}
	}

	return resultIDs, r.Err()
}

func (m *TableStorage) FindAllData(ctx context.Context, profIDs []ytprof.ProfID) ([]ytprof.ProfileData, error) {
	interfaceIDs := make([]interface{}, len(profIDs))
	for index, nextID := range profIDs {
		interfaceIDs[index] = ytprof.ProfileData{ProfIDHigh: nextID.ProfIDHigh, ProfIDLow: nextID.ProfIDLow}
	}

	r, err := m.yc.LookupRows(ctx, m.tableData, interfaceIDs, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	resultIDs := make([]ytprof.ProfileData, 0)

	for r.Next() {
		var nextVal ytprof.ProfileData
		err = r.Scan(&nextVal)
		if err != nil {
			return nil, err
		}

		resultIDs = append(resultIDs, nextVal)
	}

	return resultIDs, r.Err()
}

func (m *TableStorage) FindData(ctx context.Context, profID ytprof.ProfID) (data ytprof.ProfileData, err error) {
	var r yt.TableReader
	r, err = m.yc.LookupRows(ctx, m.tableData,
		[]interface{}{ytprof.ProfileData{ProfIDHigh: profID.ProfIDHigh, ProfIDLow: profID.ProfIDLow}}, nil)
	if err != nil {
		return
	}
	defer r.Close()

	if r.Next() {
		err = r.Scan(&data)
		return
	}

	if err = r.Err(); err != nil {
		return
	}

	err = errRowNotFound(m.tableData, profID)
	return
}

func (m *TableStorage) FindProfile(ctx context.Context, profID ytprof.ProfID) (*profile.Profile, error) {
	data, err := m.FindData(ctx, profID)
	if err != nil {
		return nil, err
	}
	return profile.ParseData(data.Data)
}

func (m *TableStorage) FindProfiles(ctx context.Context, profIDs []ytprof.ProfID) ([]*profile.Profile, error) {
	data, err := m.FindAllData(ctx, profIDs)
	if err != nil {
		return nil, err
	}
	resultProfiles := make([]*profile.Profile, len(data))
	for index, next := range data {
		nextProfile, err := profile.ParseData(next.Data)
		if err != nil {
			return resultProfiles, err
		}
		resultProfiles[index] = nextProfile
	}

	return resultProfiles, nil
}

func timeElapsed(t *time.Time) time.Duration {
	elapsed := time.Since(*t)
	*t = time.Now()
	return elapsed
}

func (m *TableStorage) FindAndMergeQueryExpr(ctx context.Context, metaquery Metaquery) (*profile.Profile, error) {
	timeCur := time.Now()

	resp, err := m.MetadataIdsQueryExpr(ctx, metaquery)
	if err != nil {
		m.l.Error("metaquery failed", log.Error(err))
		return nil, err
	}

	m.l.Debug("metaquery succeeded",
		log.Int("size", len(resp)),
		log.String("time", timeElapsed(&timeCur).String()))

	profiles, err := m.FindProfiles(ctx, resp)
	if err != nil {
		m.l.Error("getting data by IDs failed", log.Error(err))
		return nil, err
	}

	m.l.Debug("profiles loaded",
		log.Int("size", len(profiles)),
		log.String("time", timeElapsed(&timeCur).String()))

	// removing tid before merge

	for _, profile := range profiles {
		for _, sample := range profile.Sample {
			delete(sample.Label, "tid")
			delete(sample.NumLabel, "tid")
		}
	}

	mergedProfile, err := profile.Merge(profiles)
	if err != nil {
		m.l.Error("merging profiles failed", log.Error(err))
		return nil, err
	}

	m.l.Debug("profiles merged",
		log.Int("size", len(profiles)),
		log.String("time", timeElapsed(&timeCur).String()))

	return mergedProfile, nil
}
