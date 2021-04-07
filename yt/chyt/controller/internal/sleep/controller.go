package sleep

import "a.yandex-team.ru/yt/go/yson"

type Controller struct {
	alias string
}

func (*Controller) Prepare(alias string, speclet yson.RawValue) (
	spec map[string]interface{}, description map[string]interface{}, annotations map[string]interface{}, err error) {
	err = nil
	spec = map[string]interface{}{
		"tasks": map[string]interface{}{
			"main": map[string]interface{}{
				"command":   "sleep 10000",
				"job_count": 1,
			},
		}}
	description = map[string]interface{}{
		"sleeper_foo": "I am sleeper, look at me!",
	}
	annotations = map[string]interface{}{
		"sleeper_bar": "Actually I'd like to wake up :)",
	}
	return
}

func (*Controller) Family() string {
	return "sleep"
}
