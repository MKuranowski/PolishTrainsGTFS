// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package secret

import (
	"fmt"
	"os"
	"strings"
)

type MissingEnvironmentKey string

func (k MissingEnvironmentKey) Error() string {
	return fmt.Sprintf("%s environment variable not set", string(k))
}

func FromEnvironment(key string) (string, error) {
	value := os.Getenv(key)
	path := os.Getenv(key + "_FILE")
	if value == "" && path != "" {
		content, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		value = string(content)
	}

	if value == "" {
		return "", MissingEnvironmentKey(key)
	}
	return strings.TrimSpace(value), nil
}
