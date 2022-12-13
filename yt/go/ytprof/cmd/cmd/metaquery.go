package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/google/pprof/profile"
	"github.com/spf13/cobra"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof"

	"a.yandex-team.ru/yt/go/ytprof/internal/app"
	"a.yandex-team.ru/yt/go/ytprof/internal/storage"
)

const (
	defaultNone = "none"
)

var (
	flagProfileGUID          string
	flagProfileType          string
	flagProfileCluster       string
	flagProfileService       string
	flagProfileHost          string
	flagProfileName          string
	flagMetadataDisplayLimit int
	flagDisplayLink          bool
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

var pushDataCmd = &cobra.Command{
	Use:   "push",
	Short: "pushes profile from given file to the table and returns a display link",
	RunE:  pushData,
	Args:  cobra.ExactArgs(0),
}

func init() {
	getDataCmd.Flags().StringVar(&flagProfileGUID, "guid", "", "guid of the profiles")
	findDataCmd.Flags().StringVar(&flagProfileType, "type", "cpu", "profile type")
	pushDataCmd.Flags().StringVar(&flagProfileType, "type", "cpu", "profile type")
	pushDataCmd.Flags().StringVar(&flagProfileHost, "host", defaultNone, "host of profile")
	pushDataCmd.Flags().StringVar(&flagProfileName, "name", defaultNone, "name of profile")
	pushDataCmd.Flags().StringVar(&flagProfileCluster, "cluster", defaultNone, "cluster of profile, run subcommand 'clusters', to see supported")
	pushDataCmd.Flags().StringVar(&flagProfileService, "service", defaultNone, "service of profile, run subcommand 'services', to see supported")
	findMetadataCmd.Flags().IntVar(&flagMetadataDisplayLimit, "displaylimit", 100, "max number of metadata elements to display")
	findMetadataCmd.Flags().BoolVar(&flagDisplayLink, "link", false, "display links to profiles, only for manual storage")

	rootCmd.AddCommand(getDataCmd)
	rootCmd.AddCommand(findDataCmd)
	rootCmd.AddCommand(pushDataCmd)
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
	fmt.Printf("%d\t - Total\n", len(metadataSlice))

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

		fmt.Printf("%d\t - Total of %v\n", totalOfParam, key)

		for id, kv := range toSort {
			if id == flagStatsMax {
				fmt.Printf("\t%d\t - ...\n", totalOfParam)
				break
			}
			fmt.Printf("\t%d\t - of %v.\n", kv.Value, kv.Key)
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
		Query:       flagMetaquery,
		QueryLimit:  flagQueryLimit,
		ResultSkip:  0,
		ResultLimit: 0,
		Period:      period.storagePeriod(),
	}
}

func findMetadata(cmd *cobra.Command, args []string) error {
	l := ytlog.Must()

	s, err := storage.NewTableStorageMigrate(YT, ypath.Path(flagTablePath), l)
	if err != nil {
		l.Fatal("storage creation failed", log.Error(err))
		return err
	}

	metaquery := getMetaquery(flagMetaquery, l)

	resp, err := s.MetadataQueryExpr(context.Background(), metaquery, flagIgnoreErrors)

	if err != nil {
		l.Fatal("metaquery failed", log.Error(err))
		return err
	}

	for id, metadata := range resp {
		if id == flagMetadataDisplayLimit {
			fmt.Printf("... %d more left\n", len(resp)-flagMetadataDisplayLimit)
			break
		}
		fmt.Println(metadata.String())
		if flagDisplayLink {
			fmt.Println(getProfileLink(app.UIManualRequestPrefix, ytprof.GUIDFormProfID(metadata.ProfID())))
		}
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
	l := ytlog.Must()

	s, err := storage.NewTableStorageMigrate(YT, ypath.Path(flagTablePath), l)
	if err != nil {
		l.Fatal("storage creation failed", log.Error(err))
		return err
	}

	metaquery := getMetaquery(metaqueryWithProfileType(flagMetaquery, flagProfileType), l)

	mergedProfile, err := s.FindAndMergeQueryExpr(context.Background(), metaquery, flagIgnoreErrors)
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
		resp, err := s.MetadataQueryExpr(context.Background(), metaquery, flagIgnoreErrors)

		if err != nil {
			l.Fatal("metaquery failed", log.Error(err))
			return err
		}

		displayStats(resp)
	}

	return mergedProfile.Write(file)
}

func getData(cmd *cobra.Command, args []string) error {
	l := ytlog.Must()

	profileGUID, err := guid.ParseString(flagProfileGUID)
	if err != nil {
		l.Error("parsing of GUID failed", log.Error(err), log.String("GUID", flagProfileGUID))
		return err
	}

	s, err := storage.NewTableStorageMigrate(YT, ypath.Path(flagTablePath), l)
	if err != nil {
		l.Error("storage creation failed", log.Error(err))
		return err
	}

	profile, err := s.FindProfile(context.Background(), ytprof.ProfIDFromGUID(profileGUID))
	if err != nil {
		l.Error("get profile query failed", log.Error(err))
		return err
	}

	file, err := os.OpenFile(flagFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		l.Error("opening file failed", log.Error(err), log.String("file_path", flagFilePath))
		return err
	}
	defer file.Close()

	l.Debug("preparing to write to file")

	return profile.Write(file)
}

func getProfileLink(prefix string, guid guid.GUID) string {
	return fmt.Sprintf("https://ytprof.yt.yandex-team.ru%s/%s/", app.UIManualRequestPrefix, guid)
}

func pushData(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	l := ytlog.Must()

	s, err := storage.NewTableStorageMigrate(YT, ypath.Path(flagTablePath), l)
	if err != nil {
		l.Fatal("storage creation failed", log.Error(err))
	}

	f, err := os.Open(flagFilePath)
	if err != nil {
		l.Error("can't open file", log.Error(err), log.String("path", flagFilePath))
		return err
	}

	curProfile, err := profile.Parse(f)
	if err != nil {
		l.Error("parsing profile failed", log.Error(err))
		return err
	}

	rand.Seed(time.Now().UnixMicro())

	guids, err := s.PushData(
		ctx,
		[]*profile.Profile{curProfile},
		[]string{flagProfileHost},
		flagProfileType,
		flagProfileCluster,
		flagProfileService,
		map[string]string{"Name": flagProfileName},
	)
	if err != nil {
		l.Error("pushing data failed", log.Error(err))
		return err
	}
	if len(guids) != 1 {
		l.Error("wrong response length", log.Int("length", len(guids)))
		return fmt.Errorf("wrong response length")
	}

	fmt.Println("Link to view your profile:")
	fmt.Println(getProfileLink(app.UIManualRequestPrefix, guids[0]))

	return nil
}
