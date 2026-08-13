package warc

import (
	"fmt"

	"github.com/google/uuid"
)

// UUIDVersion selects the UUID version used for generated WARC record IDs.
type UUIDVersion string

const (
	UUIDv4 UUIDVersion = "v4"
	UUIDv7 UUIDVersion = "v7"
)

func newUUID(version UUIDVersion) (string, error) {
	var (
		id  uuid.UUID
		err error
	)
	switch version {
	case "", UUIDv7:
		id, err = uuid.NewV7()
	case UUIDv4:
		id, err = uuid.NewRandom()
	default:
		return "", fmt.Errorf("invalid UUID version: %s", version)
	}
	if err != nil {
		return "", fmt.Errorf("generate UUID %s: %w", version, err)
	}
	return id.String(), nil
}
