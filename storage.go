package chord

import ()

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, string) error
	Delete(string) error
}
