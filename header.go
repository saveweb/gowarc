package warc

import "strings"

type Header map[string][]string

func (h Header) Set(key, value string) {
	h[key] = []string{value}
}

func (h Header) Add(key, value string) {
	h[key] = append(h[key], value)
}

func (h Header) Get(key string) string {
	lowerKey := strings.ToLower(key)
	for k, vv := range h {
		if strings.ToLower(k) == lowerKey {
			if len(vv) > 0 {
				return vv[0]
			}
			return ""
		}
	}
	return ""
}

func (h Header) Values(key string) []string {
	lowerKey := strings.ToLower(key)
	for k, vv := range h {
		if strings.ToLower(k) == lowerKey {
			return vv
		}
	}
	return nil
}

func (h Header) Del(key string) {
	lowerKey := strings.ToLower(key)
	for k := range h {
		if strings.ToLower(k) == lowerKey {
			delete(h, k)
			return
		}
	}
}

func NewHeader() Header {
	return make(map[string][]string)
}
