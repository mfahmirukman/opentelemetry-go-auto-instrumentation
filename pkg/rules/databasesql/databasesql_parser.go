// Copyright (c) 2024 Alibaba Group Holding Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databasesql

import (
	"errors"
	nurl "net/url"
	"strings"
)

func parseDSN(driverName, dsn string) (addr string, err error) {
	// TODO: need a more delegate DFA
	switch driverName {
	case "mysql":
		return parseMySQL(dsn)
	case "postgres":
		fallthrough
	case "postgresql":
		fallthrough
	case "pgx":
		return parsePostgres(dsn)
	}

	return "", errors.New("invalid DSN")
}

func parsePostgres(dsn string) (addr string, err error) {
	// Try URL format: postgres://user:pass@host:port/db
	u, err := nurl.Parse(dsn)
	if err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		return u.Host, nil
	}

	// Fall back to lib/pq key=value format: host=myhost port=5432 user=foo ...
	return parsePostgresKV(dsn)
}

// parsePostgresKV parses the lib/pq key=value DSN format used by GORM's postgres driver.
// Example: "host=localhost port=5432 user=foo password=bar dbname=mydb sslmode=disable"
func parsePostgresKV(dsn string) (string, error) {
	host := ""
	port := ""

	i, n := 0, len(dsn)
	for i < n {
		// skip whitespace
		for i < n && dsn[i] == ' ' {
			i++
		}
		if i >= n {
			break
		}
		// read key
		eqIdx := strings.IndexByte(dsn[i:], '=')
		if eqIdx < 0 {
			break
		}
		key := strings.TrimSpace(dsn[i : i+eqIdx])
		i += eqIdx + 1

		// read value (possibly single-quoted)
		var val string
		if i < n && dsn[i] == '\'' {
			i++ // skip opening quote
			start := i
			for i < n {
				if dsn[i] == '\\' {
					i += 2
					continue
				}
				if dsn[i] == '\'' {
					break
				}
				i++
			}
			val = dsn[start:i]
			if i < n {
				i++ // skip closing quote
			}
		} else {
			start := i
			for i < n && dsn[i] != ' ' {
				i++
			}
			val = dsn[start:i]
		}

		switch key {
		case "host":
			host = val
		case "port":
			port = val
		}
	}

	if host == "" {
		return "unknown-host", nil
	}
	if port == "" {
		port = "29999" // Unknown PostgreSQL default port
	}
	return host + ":" + port, nil
}

func parseMySQL(dsn string) (addr string, err error) {
	n := len(dsn)
	i, j := -1, -1
	for k := 0; k < n; k++ {
		if dsn[k] == '(' {
			i = k
		}
		if dsn[k] == ')' {
			j = k
			break
		}
	}
	if i >= 0 && j > i {
		return dsn[i+1 : j], nil
	}
	return "", errors.New("invalid MySQL DSN")
}
