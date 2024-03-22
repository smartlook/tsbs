package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

// loader.DBCreator interface implementation
type dbCreator struct {
	ds      targets.DataSource
	headers *common.GeneratedDataHeaders
	connStr string
	config  *ClickhouseConfig
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	// fills dbCreator struct with data structure (tables description)
	// specified at the beginning of the data file
	d.headers = d.ds.Headers()
}

func connect(
	c *ClickhouseConfig,
	db bool,
) (driver.Conn, error) {
	d := "default"

	if db {
		d = c.DbName
	}

	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			// TODO: add support for multiple addresses
			Addr: []string{fmt.Sprintf("%s:9000", c.Host)},
			Auth: clickhouse.Auth{
				Database: d,
				Username: c.User,
				Password: c.Password,
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "tsbs", Version: "0.1"},
				},
			},
			MaxOpenConns: 999,
			MaxIdleConns: 999,
			DialTimeout:  time.Second * 60,

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			// TLS: &tls.Config{
			// 	InsecureSkipVerify: true,
			// },
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

// loader.DBCreator interface implementation
func (d *dbCreator) DBExists(dbName string) bool {
	conn, err := connect(d.config, false)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rows, err := conn.Query(context.Background(), "SELECT name, engine FROM system.databases WHERE name = ?", dbName)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var name, engine string
		if err := rows.Scan(&name, &engine); err != nil {
			panic(err)
		}

		if name == dbName {
			return true
		}
	}

	return false
}

// loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error {
	conn, err := connect(d.config, false)

	if err != nil {
		panic(err)
	}
	defer conn.Close()

	sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)
	if err := conn.Exec(context.Background(), sql); err != nil {
		panic(err)
	}
	return nil
}

// loader.DBCreator interface implementation
func (d *dbCreator) CreateDB(dbName string) error {
	// Connect to ClickHouse in general and CREATE DATABASE
	conn, err := connect(d.config, false)
	if err != nil {
		panic(err)
	}

	sql := fmt.Sprintf("CREATE DATABASE %s", dbName)
	err = conn.Exec(context.Background(), sql)
	if err != nil {
		panic(err)
	}
	conn.Close()

	// Connect to specified database within ClickHouse
	conn, err = connect(d.config, true)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	createTagsTable(d.config, conn, d.headers.TagKeys, d.headers.TagTypes)
	if tableCols == nil {
		tableCols = make(map[string][]string)
	}
	tableCols["tags"] = d.headers.TagKeys
	tagColumnTypes = d.headers.TagTypes

	for tableName, fieldColumns := range d.headers.FieldKeys {
		//tableName: cpu
		// fieldColumns content:
		// usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
		createMetricsTable(d.config, conn, tableName, fieldColumns)
	}

	return nil
}

// createTagsTable builds CREATE TABLE SQL statement and runs it
func createTagsTable(conf *ClickhouseConfig, conn driver.Conn, tagNames, tagTypes []string) {
	sql := generateTagsTableQuery(tagNames, tagTypes)
	if conf.Debug > 0 {
		fmt.Printf(sql)
	}

	if err := conn.Exec(context.Background(), sql); err != nil {
		panic(err)
	}
}

// createMetricsTable builds CREATE TABLE SQL statement and runs it
func createMetricsTable(conf *ClickhouseConfig, conn driver.Conn, tableName string, fieldColumns []string) {
	tableCols[tableName] = fieldColumns

	// We'll have some service columns in table to be created and columnNames contains all column names to be created
	var columnNames []string

	if conf.InTableTag {
		// First column in the table - service column - partitioning field
		partitioningColumn := tableCols["tags"][0] // would be 'hostname'
		columnNames = append(columnNames, partitioningColumn)
	}

	// Add all column names from fieldColumns into columnNames
	columnNames = append(columnNames, fieldColumns...)

	// columnsWithType - column specifications with type. Ex.: "cpu_usage Float64"
	var columnsWithType []string
	for _, column := range columnNames {
		if len(column) == 0 {
			// Skip nameless columns
			continue
		}
		columnsWithType = append(columnsWithType, fmt.Sprintf("%s Nullable(Float64)", column))
	}

	sql := fmt.Sprintf(`
			CREATE TABLE %s (
				created_date    Date     DEFAULT today(),
				created_at      DateTime DEFAULT now(),
				time            String,
				tags_id         UInt32,
				%s
			) ENGINE = MergeTree() PARTITION BY toYYYYMM(created_date)
			ORDER BY (tags_id, created_at)
			SETTINGS index_granularity = 8192;
			`,
		tableName,
		strings.Join(columnsWithType, ","))
	if conf.Debug > 0 {
		fmt.Printf(sql)
	}

	if err := conn.Exec(context.Background(), sql); err != nil {
		panic(err)
	}
}

func generateTagsTableQuery(tagNames, tagTypes []string) string {
	// prepare COLUMNs specification for CREATE TABLE statement
	// all columns would be of the type specified in the tags header
	// e.g. tags, tag2 string,tag2 int32...
	if len(tagNames) != len(tagTypes) {
		panic("wrong number of tag names and tag types")
	}

	tagColumnDefinitions := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		tagType := serializedTypeToClickHouseType(tagName, tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, tagType)
	}

	cols := strings.Join(tagColumnDefinitions, ",\n")

	index := "id"

	return fmt.Sprintf(
		"CREATE TABLE event(\n"+
			"created_date Date     DEFAULT today(),\n"+
			"created_at   DateTime DEFAULT now(),\n"+
			"id           UInt32,\n"+
			"%s"+
			") ENGINE = MergeTree() PARTITION BY toYYYYMM(created_date)"+
			"ORDER BY (%s)"+
			"SETTINGS index_granularity = 8192;",
		cols,
		index)
}

func serializedTypeToClickHouseType(colName, serializedType string) string {
	if colName == "properties_map" {
		return "Map(LowCardinality(String), String) CODEC(ZSTD(1))"
	}
	if colName == "properties_json" {
		return "String"
	}

	switch serializedType {
	case "string":
		return "Nullable(String)"
	case "float32":
		return "Nullable(Float32)"
	case "float64":
		return "Nullable(Float64)"
	case "int64":
		return "Nullable(Int64)"
	case "int32":
		return "Nullable(Int32)"
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}
