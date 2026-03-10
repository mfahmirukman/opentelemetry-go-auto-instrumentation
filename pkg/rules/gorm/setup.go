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

package gorm

import (
	"context"
	nurl "net/url"
	"os"
	"reflect"
	"strings"
	_ "unsafe"

	"github.com/alibaba/loongsuite-go-agent/pkg/api"
	driver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

var contextKey = "otel-context"
var requestKey = "otel-request"

type gormInnerEnabler struct {
	enabled bool
}

func (g gormInnerEnabler) Enable() bool {
	return g.enabled
}

var gormEnabler = gormInnerEnabler{os.Getenv("OTEL_INSTRUMENTATION_GORM_ENABLED") != "false"}

var gormInstrumenter = BuildGormInstrumenter()

//go:linkname afterGormOpen gorm.io/gorm.afterGormOpen
func afterGormOpen(call api.CallContext, db *gorm.DB, err error) {
	if !gormEnabler.Enable() {
		return
	}
	if err != nil || db == nil {
		return
	}
	// Propagate DB info to the underlying sql.DB so the databasesql
	// instrumentation layer can populate server.address and db.system.name.
	// This is necessary when the GORM driver uses sql.OpenDB() (e.g.
	// gorm.io/driver/postgres with pgx) which bypasses the sql.Open() hook.
	propagateDbInfoToSqlDB(db)

	// add the callback
	_ = db.Callback().Create().Before("gorm:create").Register("otel_create_create_span", beforeCallback("", "create"))
	_ = db.Callback().Query().Before("gorm:query").Register("otel_create_query_span", beforeCallback("", "query"))
	_ = db.Callback().Update().Before("gorm:update").Register("otel_create_update_span", beforeCallback("", "update"))
	_ = db.Callback().Delete().Before("gorm:delete").Register("otel_create_delete_span", beforeCallback("", "delete"))
	_ = db.Callback().Row().Before("gorm:row").Register("otel_create_row_span", beforeCallback("", "row"))
	_ = db.Callback().Raw().Before("gorm:raw").Register("otel_create_raw_span", beforeCallback("", "raw"))

	// after database operation
	_ = db.Callback().Create().After("gorm:create").Register("otel_end_create_span", afterCallback(""))
	_ = db.Callback().Query().After("gorm:query").Register("otel_end_query_span", afterCallback(""))
	_ = db.Callback().Update().After("gorm:update").Register("otel_end_update_span", afterCallback(""))
	_ = db.Callback().Delete().After("gorm:delete").Register("otel_end_delete_span", afterCallback(""))
	_ = db.Callback().Row().After("gorm:row").Register("otel_end_row_span", afterCallback(""))
	_ = db.Callback().Raw().After("gorm:raw").Register("otel_end_raw_span", afterCallback(""))
}

func beforeCallback(endpoint string, op string) func(db *gorm.DB) {
	return func(db *gorm.DB) {
		dbName, addr, system, user := getDbInfo(db.Config.Dialector)
		request := gormRequest{
			DbName:    dbName,
			Endpoint:  addr,
			Operation: op,
			User:      user,
			System:    system,
		}
		parentCtx := db.Statement.Context
		if parentCtx == nil {
			parentCtx = context.Background()
		}
		ctx := gormInstrumenter.Start(parentCtx, request)
		db.Set(contextKey, ctx)
		db.Set(requestKey, request)
	}
}

func afterCallback(endpoint string) func(db *gorm.DB) {
	return func(db *gorm.DB) {
		iCtx, ok := db.Get(contextKey)
		if !ok {
			return
		}
		ctx, ok := iCtx.(context.Context)
		if !ok {
			return
		}
		iRequest, ok := db.Get(requestKey)
		if !ok {
			return
		}
		request, ok := iRequest.(gormRequest)
		if !ok {
			return
		}
		gormInstrumenter.End(ctx, request, nil, db.Statement.Error)
	}
}

func propagateDbInfoToSqlDB(db *gorm.DB) {
	sqlDB, err := db.DB()
	if err != nil || sqlDB == nil {
		return
	}
	// Only set fields if they are not already populated (e.g. by the
	// databasesql sql.Open() hook).
	if sqlDB.Endpoint != "" {
		return
	}
	dsn, driverName, dbSystem := extractDialectorInfo(db.Config.Dialector)
	if dsn == "" {
		return
	}
	switch dbSystem {
	case "postgres":
		if driverName == "" {
			driverName = "pgx"
		}
		_, addr, _ := parsePostgresDSN(dsn)
		sqlDB.Endpoint = addr
		sqlDB.DriverName = driverName
		sqlDB.DSN = dsn
	case "mysql":
		cfg, parseErr := driver.ParseDSN(dsn)
		if parseErr != nil {
			return
		}
		sqlDB.Endpoint = cfg.Addr
		sqlDB.DriverName = "mysql"
		sqlDB.DSN = dsn
	}
}

func getDbInfo(dial gorm.Dialector) (string, string, string, string) {
	dsn, _, dbSystem := extractDialectorInfo(dial)
	if dsn == "" {
		return "", "", "", ""
	}
	switch dbSystem {
	case "postgres":
		dbName, addr, user := parsePostgresDSN(dsn)
		return dbName, addr, "postgresql", user
	case "mysql":
		cfg, err := driver.ParseDSN(dsn)
		if err != nil {
			return "", "", "", ""
		}
		return cfg.DBName, cfg.Addr, "mysql", cfg.User
	default:
		return "", "", "", ""
	}
}

// extractDialectorInfo uses reflection to extract DSN, DriverName, and the
// database system kind from any GORM dialector without importing the concrete
// driver packages. This avoids type-assertion failures caused by version
// mismatches between the instrumentation module and the user's dependencies.
func extractDialectorInfo(dial gorm.Dialector) (dsn, driverName, dbSystem string) {
	if dial == nil {
		return
	}

	// Identify the database system from the concrete type name,
	// e.g. "*postgres.Dialector" or "*mysql.Dialector".
	typeName := reflect.TypeOf(dial).String()
	switch {
	case strings.Contains(typeName, "postgres"):
		dbSystem = "postgres"
	case strings.Contains(typeName, "mysql"):
		dbSystem = "mysql"
	default:
		return
	}

	// Navigate: Dialector → embedded *Config → DSN / DriverName fields.
	v := reflect.ValueOf(dial)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	configField := v.FieldByName("Config")
	if !configField.IsValid() {
		return
	}
	if configField.Kind() == reflect.Ptr {
		if configField.IsNil() {
			return
		}
		configField = configField.Elem()
	}
	if dsnField := configField.FieldByName("DSN"); dsnField.IsValid() && dsnField.Kind() == reflect.String {
		dsn = dsnField.String()
	}
	if dnField := configField.FieldByName("DriverName"); dnField.IsValid() && dnField.Kind() == reflect.String {
		driverName = dnField.String()
	}
	return
}

func parsePostgresDSN(dsn string) (dbName, addr, user string) {
	// Try URL format: postgres://user:pass@host:port/dbname?sslmode=disable
	u, err := nurl.Parse(dsn)
	if err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		dbName = strings.TrimPrefix(u.Path, "/")
		addr = u.Host
		user = u.User.Username()
		return
	}

	// Fall back to key=value format: host=localhost port=5432 user=foo dbname=mydb
	var host, port string
	for _, part := range strings.Fields(dsn) {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "host":
			host = kv[1]
		case "port":
			port = kv[1]
		case "user":
			user = kv[1]
		case "dbname":
			dbName = kv[1]
		}
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5432"
	}
	addr = host + ":" + port
	return
}
