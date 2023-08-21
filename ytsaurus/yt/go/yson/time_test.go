package yson

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalTime(t *testing.T) {
	t0 := time.Now()
	b, err := MarshalTime(Time(t0))
	require.NoError(t, err)

	data, err := Marshal(Time(t0))
	require.NoError(t, err)

	assert.Equal(t, fmt.Sprintf("%q", b), string(data))
}

func TestDecodeTime(t *testing.T) {
	t0 := time.Now().UTC().Round(time.Microsecond)

	b, _ := t0.MarshalText()
	_ = t0.UnmarshalText(b)

	// testRoundtrip(t, t0)
}

func TestZeroTime(t *testing.T) {
	t0 := Time{}

	s, err := MarshalTime(t0)
	require.NoError(t, err)
	require.Equal(t, "#", s)
	t.Logf("zero time (%v) encoded into %s", t0, s)

	t1, err := UnmarshalTime(s)
	require.NoError(t, err)
	require.Equal(t, Time{}, t1)
	t.Logf("entity decoded into %v", t1)
}

func TestTimeFormat(t *testing.T) {
	before := "2019-03-09T13:27:04.084029Z"

	t0, err := UnmarshalTime(before)
	require.NoError(t, err)

	t1 := time.Time(t0)

	assert.Equal(t, 2019, t1.Year())
	assert.Equal(t, time.March, t1.Month())
	assert.Equal(t, 9, t1.Day())
	assert.Equal(t, 13, t1.Hour())
	assert.Equal(t, 27, t1.Minute())
	assert.Equal(t, 04, t1.Second())
	assert.Equal(t, 84029000, t1.Nanosecond())

	after, err := MarshalTime(t0)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestTimeFormatIntegerSeconds(t *testing.T) {
	before := "2019-03-09T13:27:04.000000Z"

	t0, err := UnmarshalTime(before)
	require.NoError(t, err)

	t1 := time.Time(t0)

	assert.Equal(t, 2019, t1.Year())
	assert.Equal(t, time.March, t1.Month())
	assert.Equal(t, 9, t1.Day())
	assert.Equal(t, 13, t1.Hour())
	assert.Equal(t, 27, t1.Minute())
	assert.Equal(t, 04, t1.Second())
	assert.Equal(t, 0, t1.Nanosecond())

	after, err := MarshalTime(t0)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestDecodeDuration(t *testing.T) {
	ys, _ := Marshal(15000)

	var d Duration
	require.NoError(t, Unmarshal(ys, &d))
	assert.Equal(t, d, Duration(15*time.Second))

	ys2, err := Marshal(d)
	require.NoError(t, err)
	assert.Equal(t, ys, ys2)
}
