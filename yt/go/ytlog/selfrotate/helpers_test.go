package selfrotate

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextRotation(t *testing.T) {
	const format = "2006-01-02T15:04:05"

	for _, testCase := range []struct {
		Now, Next string
		Interval  RotateInterval
	}{
		{
			Now:      "2020-01-22T14:54:23",
			Next:     "2020-01-22T15:00:00",
			Interval: RotateHourly,
		},
		{
			Now:      "2020-01-22T14:54:23",
			Next:     "2020-01-23T00:00:00",
			Interval: RotateDaily,
		},
	} {
		t.Run(fmt.Sprintf("%s/%s", testCase.Now, time.Duration(testCase.Interval)), func(t *testing.T) {
			tNow, err := time.Parse(format, testCase.Now)
			require.NoError(t, err)

			tNext, err := time.Parse(format, testCase.Next)
			require.NoError(t, err)

			require.Equal(t, tNext, nextRotation(tNow, testCase.Interval))
		})
	}
}

func TestFileIndex(t *testing.T) {
	for _, testCase := range []struct {
		File, Pattern string
		Ok            bool
		LogFile       logFile
	}{
		{
			File:    "a.log",
			Pattern: "a.log",
			Ok:      true,
			LogFile: logFile{Name: "a.log", Index: 0},
		},
		{
			File:    "a.log.gz",
			Pattern: "a.log",
			Ok:      true,
			LogFile: logFile{Name: "a.log.gz", Index: 0, Compressed: true},
		},
		{
			File:    "a.log.gz.tmp",
			Pattern: "a.log",
			Ok:      false,
		},
		{
			File:    "a.log.1.gz",
			Pattern: "a.log",
			Ok:      true,
			LogFile: logFile{Name: "a.log.1.gz", Index: 1, Compressed: true},
		},
		{
			File:    "a.log.1",
			Pattern: "a.log",
			Ok:      true,
			LogFile: logFile{Name: "a.log.1", Index: 1},
		},
		{
			File:    "a.log.999.gz",
			Pattern: "a.log",
			Ok:      true,
			LogFile: logFile{Name: "a.log.999.gz", Index: 999, Compressed: true},
		},
		{
			File:    "a.log.999",
			Pattern: "a.log",
			Ok:      true,
			LogFile: logFile{Name: "a.log.999", Index: 999},
		},
	} {
		t.Run(testCase.File, func(t *testing.T) {
			logFile, ok := parseFilename(testCase.File, testCase.Pattern)
			if testCase.Ok {
				require.True(t, ok)
				require.Equal(t, testCase.LogFile, logFile)
			} else {
				require.False(t, ok)
			}
		})
	}
}
