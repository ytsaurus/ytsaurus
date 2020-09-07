package ratelimit

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseRate parses rate in form of "N/D", e.g "10/1s" or "100/1ms"
func ParseRate(rate string) (count int, interval time.Duration, err error) {
	parts := strings.SplitN(rate, "/", 2)
	if len(parts) != 2 {
		err = fmt.Errorf("invalid rate format in %q: missing slash", rate)
		return
	}

	count, err = strconv.Atoi(parts[0])
	if err != nil {
		err = fmt.Errorf("invalid rate format in %q: %v", rate, err)
		return
	}

	interval, err = time.ParseDuration(parts[1])
	if err != nil {
		err = fmt.Errorf("invalid rate format in %q: %v", rate, err)
		return
	}

	if interval < 0 {
		err = fmt.Errorf("invalid rate format in %q: negative interval", rate)
		return
	}

	return
}
