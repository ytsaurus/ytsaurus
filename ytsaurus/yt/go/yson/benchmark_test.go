package yson

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	LinePath       = "testdata/line.yson"
	SimpleLinePath = "testdata/line_simple.yson"
)

func BenchmarkMarshal(b *testing.B) {
	for _, input := range []any{
		1,
		uint64(1),
		"foo",
		ValueWithAttrs{
			map[string]any{
				"foo":  "bar",
				"foo1": nil,
			},
			[]any{1, "zog", nil},
		},
	} {
		b.Run(fmt.Sprintf("%T", input), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = Marshal(input)
			}
		})
	}
}

func RunBenchmark(b *testing.B, do func(input []byte, typ Type) error) {
	for _, format := range []Format{FormatBinary, FormatText} {
		for _, typ := range []Type{TypeString, TypeInt64, TypeFloat64} {
			b.Run(fmt.Sprint(format, "/", typ), func(b *testing.B) {
				var input bytes.Buffer
				w := NewWriterFormat(&input, format)

				w.BeginList()
				w.BeginMap()
				for i := 0; input.Len() < 10*1024*1024; i++ {
					if i%10 == 0 {
						w.EndMap()
						w.BeginMap()
					}

					w.MapKeyString(fmt.Sprintf("K%d", i%10))
					switch typ {
					case TypeString:
						w.String(strings.Repeat(" ", 1000))
					case TypeInt64:
						w.Int64(42)
					case TypeFloat64:
						w.Float64(3.14)
					}
				}

				w.EndMap()
				w.EndList()
				_ = w.Finish()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := do(input.Bytes(), typ); err != nil {
						b.Fatal(err)
					}
				}

				b.SetBytes(int64(input.Len()))
				b.ReportAllocs()
			})
		}
	}
}

func RunBenchmarkTestfile(b *testing.B, typ string, do func(input []byte) error) {
	b.Run(fmt.Sprint(typ, "/path_", LinePath), func(b *testing.B) {
		contents, err := os.ReadFile(LinePath)
		if err != nil {
			b.Fatal(err)
		}
		contents = contents[:len(contents)-1]

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := do(contents); err != nil {
				b.Fatal(err)
			}
		}

		b.SetBytes(int64(len(contents)))
		b.ReportAllocs()
	})
}

func BenchmarkScanner(b *testing.B) {
	RunBenchmark(b, func(input []byte, typ Type) error {
		return Valid(input)
	})
}

func BenchmarkReader(b *testing.B) {
	RunBenchmark(b, func(input []byte, typ Type) error {
		r := NewReaderFromBytes(input)
		for {
			e, err := r.Next(false)
			if err != nil {
				b.Fatal(err)
			}

			if e == EventEOF {
				return nil
			}
		}
	})
}

func BenchmarkReaderTestfile(b *testing.B) {
	RunBenchmarkTestfile(b, "reader/event", func(input []byte) error {
		r := NewReaderFromBytes(input[:len(input)-1])
		for {
			e, err := r.Next(false)
			if err != nil {
				b.Fatal(err)
			}

			if e == EventEOF {
				return nil
			}
		}
	})
}

func BenchmarkDecoderInterface(b *testing.B) {
	RunBenchmark(b, func(input []byte, typ Type) error {
		var value any
		return Unmarshal(input, &value)
	})
}

func TestUnmarshalInterfaceTestfile(t *testing.T) {
	contents, err := os.ReadFile(SimpleLinePath)
	require.NoError(t, err)

	var value any
	require.NoError(t, Unmarshal(contents[:len(contents)-1], &value))
}

func TestUnmarshalMapTestfile(t *testing.T) {
	contents, err := os.ReadFile(LinePath)
	require.NoError(t, err)

	var value map[string]any
	require.NoError(t, Unmarshal(contents[:len(contents)-1], &value))
}

func TestUnmarshalStructTestfile(t *testing.T) {
	contents, err := os.ReadFile(LinePath)
	require.NoError(t, err)

	var value ProblemTable
	require.NoError(t, Unmarshal(contents[:len(contents)-1], &value))
}

type ProblemTableSimple struct {
	TInt64 int64 `yson:"t_int64"`
}

type ProblemTable struct {
	FlightID              int64   `yson:"flightId"`
	ScrollPercent         int64   `yson:"scrollPercent"`
	ViewTimeTillEndSec    int64   `yson:"viewTimeTillEndSec"`
	APINameID             int64   `yson:"apiNameId"`
	ClickOnComment        bool    `yson:"clickOnComment"`
	ViewTimeSec           int64   `yson:"viewTimeSec"`
	IsTablet              bool    `yson:"isTablet"`
	LoadTimeMs            int64   `yson:"loadTimeMs"`
	PrestableFlag         int64   `yson:"prestableFlag"`
	ReferrerIntegrationID int64   `yson:"referrerIntegrationId"`
	SfID                  int64   `yson:"sfId"`
	XurmaRules            []any   `yson:"xurmaRules"`
	BrowserEngineVersion  string  `yson:"browserEngineVersion"`
	ClientTimestamp       string  `yson:"clientTimestamp"`
	ReferrerPartnerID     int64   `yson:"referrerPartnerId"`
	RequestTimeMs         int64   `yson:"requestTimeMs"`
	WithCommentID         int64   `yson:"withCommentId"`
	XurmaSign             int64   `yson:"xurmaSign"`
	CLID                  int64   `yson:"clid"`
	ParentSize            int64   `yson:"parentSize"`
	BrowserName           string  `yson:"browserName"`
	GeoPath               []int64 `yson:"geoPath"`
	CountryCode           string  `yson:"countryCode"`
	DeviceID              string  `yson:"deviceId"`
	IntegrationID         int64   `yson:"integrationId"`
	ItemTypeID            int64   `yson:"itemTypeId"`
	ParentItemTypeID      int64   `yson:"parentItemTypeId"`
	Pos                   int64   `yson:"pos"`
	AdProviderID          int64   `yson:"adProviderId"`
	BrowserVersion        string  `yson:"browserVersion"`
	VariantID             int64   `yson:"variantId"`
	RecVersion            string  `yson:"recVersion"`
	URL                   string  `yson:"url"`
	PartnerID             int64   `yson:"partnerId"`
	RecExperiments        []any   `yson:"recExperiments"`
	SourceItemID          int64   `yson:"sourceItemId"`
	OsFamily              string  `yson:"osFamily"`
	ParentItemID          int64   `yson:"parentItemId"`
	RID4                  int64   `yson:"rid4"`
	RID3                  int64   `yson:"rid3"`
	StrongestID           string  `yson:"strongestId"`
	UID                   int64   `yson:"uid"`
	IsFavouriteSource     bool    `yson:"isFavouriteSource"`
	IsRobot               bool    `yson:"isRobot"`
	UserVisitsCount       int64   `yson:"userVisitsCount"`
	IsMobile              bool    `yson:"isMobile"`
	UserIDTypeID          int64   `yson:"userIdTypeId"`
	PageTypeID            int64   `yson:"pageTypeId"`
	PartnerIDTypeID       int64   `yson:"partnerIdTypeId"`
	RecHostID             int64   `yson:"recHostId"`
	ReferrerProductID     int64   `yson:"referrerProductId"`
	RID1                  int64   `yson:"rid1"`
	RID2                  int64   `yson:"rid2"`
	EventID               int64   `yson:"eventId"`
	OsVersion             string  `yson:"osVersion"`
	SourceItemTypeID      int64   `yson:"sourceItemTypeId"`
	YandexUID             uint64  `yson:"yandexuid"`
	PlaceID               int64   `yson:"placeId"`
	UserClicksCount       int64   `yson:"userClicksCount"`
	BrowserEngine         string  `yson:"browserEngine"`
	Experiments           []any   `yson:"experiments"`
	ZenBranchID           int64   `yson:"zenBranchId"`
	PartnerUserID         string  `yson:"partnerUserId"`
	IsShort               bool    `yson:"isShort"`
	ItemID                int64   `yson:"itemId"`
	Staff                 bool    `yson:"staff"`
	Timestamp             string  `yson:"timestamp"`
	DC                    string  `yson:"dc"`
	ProductID             int64   `yson:"productId"`
}

func BenchmarkDecoderInterfaceTestfile(b *testing.B) {
	RunBenchmarkTestfile(b, "unmarshal/interface", func(input []byte) error {
		var value any
		return Unmarshal(input, &value)
	})
}

func BenchmarkDecoderMapTestfile(b *testing.B) {
	RunBenchmarkTestfile(b, "unmarshal/map", func(input []byte) error {
		var value map[string]any
		return Unmarshal(input, &value)
	})
}

func BenchmarkDecoderStructTestfile(b *testing.B) {
	RunBenchmarkTestfile(b, "unmarshal/struct", func(input []byte) error {
		var value ProblemTable
		err := Unmarshal(input, &value)
		return err
	})
}

type benchStructString struct {
	K0, K1, K2, K3, K4, K5, K6, K7, K8, K9 string
}

type benchStructInt struct {
	K0, K1, K2, K3, K4, K5, K6, K7, K8, K9 int64
}

type benchStructFloat struct {
	K0, K1, K2, K3, K4, K5, K6, K7, K8, K9 float64
}

func BenchmarkDecoderStruct(b *testing.B) {
	RunBenchmark(b, func(input []byte, typ Type) error {
		switch typ {
		case TypeString:
			var s []benchStructString
			return Unmarshal(input, &s)
		case TypeInt64:
			var s []benchStructInt
			return Unmarshal(input, &s)
		case TypeFloat64:
			var s []benchStructFloat
			return Unmarshal(input, &s)
		}

		return nil
	})
}
