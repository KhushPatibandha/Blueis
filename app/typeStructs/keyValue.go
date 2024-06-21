package typestructs

import "time"

type KeyValue struct {
	Value       string
	ExpiryTime  *time.Time
}