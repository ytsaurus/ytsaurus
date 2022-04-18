package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/google/pprof/profile"

	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/internal/expressions"
)

const (
	DefaultQueryLimit = 10000
)

func errRowAlreadyExists(table interface{}, rowKey interface{}) error {
	return fmt.Errorf("row with id(%v) already exists in table(%v)", rowKey, table)
}

func errRowNotFound(table interface{}, rowKey interface{}) error {
	return fmt.Errorf("row with id(%v) not found in table(%v)", rowKey, table)
}

type (
	TableStorage struct {
		tableData     ypath.Path
		tableMetadata ypath.Path
		yc            yt.Client

		l *logzap.Logger
	}
)

func NewTableStorage(yc yt.Client, path ypath.Path, l *logzap.Logger) *TableStorage {
	p := new(TableStorage)
	p.yc = yc
	p.tableData = path.Child(ytprof.TableData)
	p.tableMetadata = path.Child(ytprof.TableMetadata)
	p.l = l
	return p
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

func (m *TableStorage) PushData(profiles []*profile.Profile, hosts []string, profileType string, cluster string, service string, ctx context.Context) error {
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

func (m *TableStorage) MetadataIdsQuery(minTime schema.Timestamp, maxTime schema.Timestamp, ctx context.Context) ([]ytprof.ProfID, error) {
	r, err := m.yc.SelectRows(ctx, m.queryIDsMetadataPeriod(minTime, maxTime, DefaultQueryLimit), nil)
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

func (m *TableStorage) MetadataQuery(minTime schema.Timestamp, maxTime schema.Timestamp, ctx context.Context) ([]ytprof.ProfileMetadata, error) {
	r, err := m.yc.SelectRows(ctx, m.queryMetadataPeriod(minTime, maxTime, DefaultQueryLimit), nil)
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

func (m *TableStorage) MetadataQueryExpr(minTime schema.Timestamp, maxTime schema.Timestamp, ctx context.Context, exprText string) ([]ytprof.ProfileMetadata, error) {
	r, err := m.yc.SelectRows(ctx, m.queryMetadataPeriod(minTime, maxTime, DefaultQueryLimit), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	results := make([]ytprof.ProfileMetadata, 0)

	expr, err := expressions.NewExpression(exprText)
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

func (m *TableStorage) MetadataIdsQueryExpr(minTime schema.Timestamp, maxTime schema.Timestamp, ctx context.Context, exprText string) ([]ytprof.ProfID, error) {
	r, err := m.yc.SelectRows(ctx, m.queryMetadataPeriod(minTime, maxTime, DefaultQueryLimit), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	resultIDs := make([]ytprof.ProfID, 0)

	expr, err := expressions.NewExpression(exprText)
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

func (m *TableStorage) FindAllData(profIDs []ytprof.ProfID, ctx context.Context) ([]ytprof.ProfileData, error) {
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

func (m *TableStorage) FindData(profID ytprof.ProfID, ctx context.Context) (data ytprof.ProfileData, err error) {
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

func (m *TableStorage) FindProfile(profID ytprof.ProfID, ctx context.Context) (*profile.Profile, error) {
	data, err := m.FindData(profID, ctx)
	if err != nil {
		return nil, err
	}
	return profile.ParseData(data.Data)
}

func (m *TableStorage) FindProfiles(profIDs []ytprof.ProfID, ctx context.Context) ([]*profile.Profile, error) {
	data, err := m.FindAllData(profIDs, ctx)
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
