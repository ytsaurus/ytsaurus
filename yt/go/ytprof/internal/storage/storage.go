package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytprof"
	"github.com/google/pprof/profile"
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

		l log.Structured
	}
)

func NewTableStorage(yc yt.Client, l log.Structured, path ypath.Path) *TableStorage {
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

func (m *TableStorage) PushData(profiles []*profile.Profile, ctx context.Context) error {
	tx, err := m.yc.BeginTabletTx(ctx, nil)
	if err != nil {
		return err
	}

	rowsData := make([]interface{}, 0, len(profiles))
	rowsMetadata := make([]interface{}, 0, len(profiles))

	curTime := time.Now().UnixMilli()
	for _, profileData := range profiles {
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
			Timestamp:  curTime,
			Metadata:   ytprof.Metadata{ProfileType: profileData.DefaultSampleType},
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

func (m *TableStorage) MetadataIdsQuery(minTime int64, maxTime int64, ctx context.Context) ([]ytprof.ProfID, error) {
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

func (m *TableStorage) MetadataQuery(minTime int64, maxTime int64, ctx context.Context) ([]ytprof.ProfileMetadata, error) {
	r, err := m.yc.SelectRows(ctx, m.queryIDsMetadataPeriod(minTime, maxTime, DefaultQueryLimit), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	resultIDs := make([]ytprof.ProfileMetadata, 0)

	for r.Next() {
		var nextVal ytprof.ProfileMetadata
		err = r.Scan(&nextVal)
		if err != nil {
			return nil, err
		}

		resultIDs = append(resultIDs, nextVal)
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
