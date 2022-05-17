package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/google/pprof/profile"
	"github.com/spf13/cobra"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/internal/storage"
)

var (
	flagProfileGUID          string
	flagProfileType          string
	flagMetadataDisplayLimit int
)

var findMetadataCmd = &cobra.Command{
	Use:   "list",
	Short: "Searches for profiles witch fit the metaquery and [timestampMin, timestampMax] in tablePath/metadata",
	RunE:  findMetadata,
	Args:  cobra.ExactArgs(0),
}

var findDataCmd = &cobra.Command{
	Use:   "merge",
	Short: "Searches for profiles witch fit the metaquery and [timestampMin, timestampMax] in tablePath/data and loads them into file",
	RunE:  findData,
	Args:  cobra.ExactArgs(0),
}

var getDataCmd = &cobra.Command{
	Use:   "get",
	Short: "Searches for profile with given GUID and loads it into file",
	RunE:  getData,
	Args:  cobra.ExactArgs(0),
}

func init() {
	getDataCmd.Flags().StringVar(&flagProfileGUID, "guid", "", "guid of the profiles")
	findDataCmd.Flags().StringVar(&flagProfileType, "type", "cpu", "profile type default - cpu")
	findMetadataCmd.Flags().IntVar(&flagMetadataDisplayLimit, "displaylimit", 1000000000, "max number of metadata elements to display")

	rootCmd.AddCommand(getDataCmd)
	rootCmd.AddCommand(findDataCmd)
	rootCmd.AddCommand(findMetadataCmd)
}

func storagePreRun() (*zap.Logger, *storage.TableStorage, error) {
	l, err := ytlog.New()
	if err != nil {
		return nil, nil, err
	}

	tableYTPath := ypath.Path(flagTablePath)

	err = ytprof.MigrateTables(YT, tableYTPath)

	if err != nil {
		l.Fatal("migraton failed", log.Error(err), log.String("table_path", tableYTPath.String()))
		return nil, nil, err
	}

	storage := storage.NewTableStorage(YT, tableYTPath, l)

	return l, storage, nil
}

func displayStats(metadataSlice []ytprof.ProfileMetadata) {
	fmt.Fprintf(os.Stdout, "%d\t - Total\n", len(metadataSlice))

	metaParams := map[string]map[string]int{}

	for _, metadata := range metadataSlice {
		for key, value := range metadata.Metadata.MapData {
			if metaParams[key] == nil {
				metaParams[key] = map[string]int{}
			}
			metaParams[key][value]++
		}
	}

	type kv struct {
		Key   string
		Value int
	}

	for key, metaParam := range metaParams {
		var toSort []kv
		var totalOfParam int

		for value, valueNums := range metaParam {
			toSort = append(toSort, kv{value, valueNums})
			totalOfParam += valueNums
		}

		sort.Slice(toSort, func(i, j int) bool {
			return toSort[i].Value > toSort[j].Value
		})

		fmt.Fprintf(os.Stderr, "%d\t - Total of %v\n", totalOfParam, key)

		for id, kv := range toSort {
			if id == flagStatsMax {
				fmt.Fprintf(os.Stderr, "\t%d\t - ...\n", totalOfParam)
				break
			}
			fmt.Fprintf(os.Stderr, "\t%d\t - of %v\n", kv.Value, kv.Key)
			totalOfParam -= kv.Value
		}
	}
}

func metaQueryPreRun() (*zap.Logger, *storage.TableStorage, schema.Timestamp, schema.Timestamp, error) {
	l, storage, err := storagePreRun()
	if err != nil {
		return nil, nil, schema.Timestamp(0), schema.Timestamp(0), err
	}

	var tmin, tmax time.Time

	if flagTimeLast != "" {
		timeLast, err := time.ParseDuration(flagTimeLast)
		if err != nil {
			l.Fatal("incorect time duration format",
				log.Error(err),
				log.String("value", flagTimestampMin))
			return nil, nil, schema.Timestamp(0), schema.Timestamp(0), err
		}

		tmax = time.Now()

		tmin = tmax.Add(-timeLast)

	} else {
		tmin, err = time.Parse(ytprof.TimeFormat, flagTimestampMin)
		if err != nil {
			l.Fatal("incorect time format",
				log.Error(err),
				log.String("corret", ytprof.TimeFormat),
				log.String("actual", flagTimestampMin))
			return nil, nil, schema.Timestamp(0), schema.Timestamp(0), err
		}

		tmax, err = time.Parse(ytprof.TimeFormat, flagTimestampMax)
		if err != nil {
			l.Fatal("incorect time format",
				log.Error(err),
				log.String("corret", ytprof.TimeFormat),
				log.String("actual", flagTimestampMax))
			return nil, nil, schema.Timestamp(0), schema.Timestamp(0), err
		}
	}

	schemaTimestampMin, err := schema.NewTimestamp(tmin)
	if err != nil {
		l.Fatal("cannot generate timestamp", log.Error(err))
		return nil, nil, schema.Timestamp(0), schema.Timestamp(0), err
	}
	schemaTimestampMax, err := schema.NewTimestamp(tmax)
	if err != nil {
		l.Fatal("cannot generate timestamp", log.Error(err))
		return nil, nil, schema.Timestamp(0), schema.Timestamp(0), err
	}

	return l, storage, schemaTimestampMin, schemaTimestampMax, nil
}

func findMetadata(cmd *cobra.Command, args []string) error {
	l, storage, schemaTimestampMin, schemaTimestampMax, err := metaQueryPreRun()
	if err != nil {
		return err
	}

	resp, err := storage.MetadataQueryExpr(
		context.Background(),
		schema.Timestamp(schemaTimestampMin),
		schema.Timestamp(schemaTimestampMax),
		flagMetaquery,
		flagQueryLimit)

	if err != nil {
		l.Fatal("metaquery failed", log.Error(err))
		return err
	}

	for id, metadata := range resp {
		if id == flagMetadataDisplayLimit {
			fmt.Fprintf(os.Stderr, "... %d more left\n", len(resp)-flagMetadataDisplayLimit)
			break
		}
		fmt.Fprintln(os.Stderr, metadata.String())
	}

	if flagStats {
		displayStats(resp)
	}

	return nil
}

func metaqueryWithProfileType(metaquery string, profileType string) string {
	return "Metadata['ProfileType'] == '" + profileType + "' && (" + metaquery + ")"
}

func timeElapsed(t *time.Time) time.Duration {
	elapsed := time.Since(*t)
	*t = time.Now()
	return elapsed
}

func findData(cmd *cobra.Command, args []string) error {
	l, storage, scemaTimestampMin, scemaTimestampMax, err := metaQueryPreRun()
	if err != nil {
		return err
	}

	ctx := context.Background()

	timeCur := time.Now()

	resp, err := storage.MetadataIdsQueryExpr(
		ctx,
		schema.Timestamp(scemaTimestampMin),
		schema.Timestamp(scemaTimestampMax),
		metaqueryWithProfileType(flagMetaquery, flagProfileType),
		flagQueryLimit)
	if err != nil {
		l.Fatal("metaquery failed", log.Error(err))
		return err
	}

	l.Debug("metaquery succeeded",
		log.Int("size", len(resp)),
		log.String("time", timeElapsed(&timeCur).String()))

	profiles, err := storage.FindProfiles(resp, ctx)
	if err != nil {
		l.Fatal("getting data by IDs failed", log.Error(err))
		return err
	}

	l.Debug("profiles loaded",
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
		l.Fatal("merging profiles failed", log.Error(err))
		return err
	}

	file, err := os.Create(flagFilePath)
	if err != nil {
		l.Fatal("opening file failed", log.Error(err), log.String("file_path", flagFilePath))
		return err
	}
	defer file.Close()

	l.Debug("profiles merged",
		log.Int("size", len(profiles)),
		log.String("time", timeElapsed(&timeCur).String()))

	if flagStats {
		resp, err := storage.MetadataQueryExpr(
			context.Background(),
			schema.Timestamp(scemaTimestampMin),
			schema.Timestamp(scemaTimestampMax),
			"Metadata['ProfileType'] == '"+flagProfileType+"' && ("+flagMetaquery+")",
			flagQueryLimit)

		if err != nil {
			l.Fatal("metaquery failed", log.Error(err))
			return err
		}

		displayStats(resp)
	}

	return mergedProfile.Write(file)
}

func getData(cmd *cobra.Command, args []string) error {
	l, storage, err := storagePreRun()
	if err != nil {
		return err
	}

	ctx := context.Background()

	profileGUID, err := guid.ParseString(flagProfileGUID)
	if err != nil {
		l.Fatal("parsing of GUID failed", log.Error(err), log.String("GUID", flagProfileGUID))
		return err
	}

	profile, err := storage.FindProfile(ytprof.ProfIDFromGUID(profileGUID), ctx)
	if err != nil {
		l.Fatal("get profile query failed", log.Error(err))
		return err
	}

	file, err := os.OpenFile(flagFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		l.Fatal("opening file failed", log.Error(err), log.String("file_path", flagFilePath))
		return err
	}
	defer file.Close()

	l.Debug("preparing to write to file")

	return profile.Write(file)
}
