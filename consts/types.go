package consts

import "time"

type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	du, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(du)
	return nil
}

// IterationType specifies the types of subscription that will be iterated.
type IterationType byte

const (
	// TypeSYS represents system topic, which start with '$'.
	TypeSYS IterationType = 1 << iota
	// TypeShared represents shared topic, which start with '$share/'.
	TypeShared
	// TypeNonShared represents non-shared topic.
	TypeNonShared
	TypeAll = TypeSYS | TypeShared | TypeNonShared
)

// MatchType specifies what match operation will be performed during the iteration.
type MatchType byte

const (
	MatchName MatchType = 1 << iota
	MatchFilter
)
