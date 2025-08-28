package port

import "github.com/nobletooth/kiwi/pkg/storage"

type Redis struct {
	store storage.KeyValueHolder
}
