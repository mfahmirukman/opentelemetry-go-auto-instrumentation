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
	"strings"
	_ "unsafe"

	"github.com/alibaba/loongsuite-go-agent/pkg/api"
	driver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
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
	switch d := db.Config.Dialector.(type) {
	case *mysql.Dialector:
		cfg, parseErr := driver.ParseDSN(d.Config.DSN)
		if parseErr != nil {
			return
		}
		sqlDB.Endpoint = cfg.Addr
		sqlDB.DriverName = "mysql"
		sqlDB.DSN = d.Config.DSN
	case *postgres.Dialector:
		_, addr, _ := parsePostgresDSN(d.Config.DSN)
		driverName := d.Config.DriverName
		if driverName == "" {
			driverName = "pgx"
		}
		sqlDB.Endpoint = addr
		sqlDB.DriverName = driverName
		sqlDB.DSN = d.Config.DSN
	}
}

func getDbInfo(dial gorm.Dialector) (string, string, string, string) {
	switch d := dial.(type) {
	case *mysql.Dialector:
		if cfg, ok := d.Config.DbInfo.(*driver.Config); ok {
			return cfg.DBName, cfg.Addr, "mysql", cfg.User
		}
		cfg, err := driver.ParseDSN(d.Config.DSN)
		if err != nil {
			return "", "", "", ""
		}
		d.Config.DbInfo = cfg
		return cfg.DBName, cfg.Addr, "mysql", cfg.User
	case *postgres.Dialector:
		dbName, addr, user := parsePostgresDSN(d.Config.DSN)
		return dbName, addr, "postgresql", user
	default:
		return "", "", "", ""
	}
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
