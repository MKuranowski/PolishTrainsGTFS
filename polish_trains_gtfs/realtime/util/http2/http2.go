// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package http2

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Error struct {
	URL, Status string
	StatusCode  int
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.URL, e.Status)
}

func Check(r *http.Response) error {
	if r.StatusCode >= 400 && r.StatusCode < 600 {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		return &Error{
			URL:        r.Request.URL.Redacted(),
			Status:     r.Status,
			StatusCode: r.StatusCode,
		}
	}
	return nil
}

func GetJSON[T any](client *http.Client, req *http.Request) (content *T, err error) {
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	} else if err = Check(resp); err != nil {
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(content)
	return
}
