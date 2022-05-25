package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof"

	//"a.yandex-team.ru/yt/go/ytprof/internal/metaquery"
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

type cmdPeriod struct {
	start time.Time
	end   time.Time
}

func newCmdPeriod(flagTimestampMin string, flagTimestampMax string) (cmdPeriod, error) {
	var period cmdPeriod
	var err error

	period.start, err = time.Parse(ytprof.TimeFormat, flagTimestampMin)
	if err != nil {
		return period, err
	}

	period.end, err = time.Parse(ytprof.TimeFormat, flagTimestampMax)

	return period, err
}

func newCmdPeriodLast(flagTimeLast string) (cmdPeriod, error) {
	var period cmdPeriod
	var err error

	timeLast, err := time.ParseDuration(flagTimeLast)
	if err != nil {
		return period, err
	}

	period.end = time.Now()

	period.start = period.end.Add(-timeLast)

	return period, err
}

func newCmdPeriodAuto(flagTimeLast string, flagTimestampMin string, flagTimestampMax string) (cmdPeriod, error) {
	if flagTimeLast == "" {
		return newCmdPeriod(flagTimestampMin, flagTimestampMax)
	} else {
		return newCmdPeriodLast(flagTimeLast)
	}
}

func (p cmdPeriod) storagePeriod() storage.TimestampPeriod {
	return storage.TimestampPeriod{
		Start: p.start,
		End:   p.end,
	}
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
			fmt.Fprintf(os.Stderr, "\t%d\t - of %v.\n", kv.Value, kv.Key)
			totalOfParam -= kv.Value
		}
	}
}

func getMetaquery(query string, l *zap.Logger) storage.Metaquery {
	period, err := newCmdPeriodAuto(flagTimeLast, flagTimestampMin, flagTimestampMax)
	if err != nil {
		l.Fatal("timestamp parsing failed", log.Error(err))
	}

	return storage.Metaquery{
		Query:      flagMetaquery,
		QueryLimit: flagQueryLimit,
		Period:     period.storagePeriod(),
	}
}

func findMetadata(cmd *cobra.Command, args []string) error {
	l, err := ytlog.New()
	if err != nil {
		return err
	}

	s, err := storage.NewTableStorageMigrate(flagTablePath, YT, l)
	if err != nil {
		l.Fatal("storage creation failed", log.Error(err))
		return err
	}

	metaquery := getMetaquery(flagMetaquery, l)

	resp, err := s.MetadataQueryExpr(context.Background(), metaquery)

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

func findData(cmd *cobra.Command, args []string) error {
	l, err := ytlog.New()
	if err != nil {
		return err
	}

	s, err := storage.NewTableStorageMigrate(flagTablePath, YT, l)
	if err != nil {
		l.Fatal("storage creation failed", log.Error(err))
		return err
	}

	metaquery := getMetaquery(metaqueryWithProfileType(flagMetaquery, flagProfileType), l)

	mergedProfile, err := s.FindAndMergeQueryExpr(context.Background(), metaquery)
	if err != nil {
		l.Fatal("metaquery.FindData failed", log.Error(err))
		return err
	}

	file, err := os.Create(flagFilePath)
	if err != nil {
		l.Fatal("opening file failed", log.Error(err), log.String("file_path", flagFilePath))
		return err
	}
	defer file.Close()

	if flagStats {
		resp, err := s.MetadataQueryExpr(context.Background(), metaquery)

		if err != nil {
			l.Fatal("metaquery failed", log.Error(err))
			return err
		}

		displayStats(resp)
	}

	return mergedProfile.Write(file)
}

func getData(cmd *cobra.Command, args []string) error {
	l, err := ytlog.New()
	if err != nil {
		return err
	}

	profileGUID, err := guid.ParseString(flagProfileGUID)
	if err != nil {
		l.Fatal("parsing of GUID failed", log.Error(err), log.String("GUID", flagProfileGUID))
		return err
	}

	s, err := storage.NewTableStorageMigrate(flagTablePath, YT, l)
	if err != nil {
		l.Fatal("storage creation failed", log.Error(err))
		return err
	}

	profile, err := s.FindProfile(context.Background(), ytprof.ProfIDFromGUID(profileGUID))
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
