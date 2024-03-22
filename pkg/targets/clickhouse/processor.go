package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/timescale/tsbs/pkg/targets"
)

// load.Processor interface implementation
type processor struct {
	conn driver.Conn
	csi  *syncCSI
	conf *ClickhouseConfig
}

// load.Processor interface implementation
func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	if doLoad {
		conn, err := connect(p.conf, true)
		if err != nil {
			panic(err)
		}

		p.conn = conn
		if hashWorkers {
			p.csi = newSyncCSI()
		} else {
			p.csi = globalSyncCSI
		}
	}
}

// load.ProcessorCloser interface implementation
func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.conn.Close()
	}
}

// load.Processor interface implementation
func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*tableArr)
	rowCnt := 0
	metricCnt := uint64(0)
	for tableName, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += p.processCSI(tableName, rows)

			if p.conf.LogBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/took.Seconds(), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0

	return metricCnt, uint64(rowCnt)
}

func newSyncCSI() *syncCSI {
	return &syncCSI{
		m:     make(map[string]int64),
		mutex: &sync.RWMutex{},
	}
}

type syncCSI struct {
	// Map hostname to tags.id for this host
	m     map[string]int64
	mutex *sync.RWMutex
}

// globalSyncCSI is used when data is not hashed by some function to a worker consistently so
// therefore all workers need to know about the same map from hostname -> tags_id
var globalSyncCSI = newSyncCSI()

// Process part of incoming data - insert into tables
func (p *processor) processCSI(tableName string, rows []*insertData) uint64 {
	tagRows := make([][]string, 0, len(rows))
	dataRows := make([][]interface{}, 0, len(rows))
	ret := uint64(0)
	commonTagsLen := len(tableCols["tags"])

	colLen := len(tableCols[tableName]) + 2
	if p.conf.InTableTag {
		colLen++
	}

	var tagsIdPosition int = 0

	for _, row := range rows {
		// Split the tags into individual common tags and
		// an extra bit leftover for non-common tags that need to be added separately.
		// For each of the common tags, remove everything after = in the form <label>=<val>
		// since we won't need it.
		// tags line ex.:
		// hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
		tags := strings.SplitN(row.tags, ",", commonTagsLen+1)
		// tags = (
		//	hostname=host_0
		//	region=eu-west-1
		//	datacenter=eu-west-1b
		// )
		// extract value of each tag
		// tags = (
		//	host_0
		//	eu-west-1
		//	eu-west-1b
		// )
		for i := 0; i < commonTagsLen; i++ {
			tags[i] = strings.Split(tags[i], "=")[1]
		}
		// prepare JSON for tags that are not common
		// var json interface{} = nil
		// if len(tags) > commonTagsLen {
		// 	// Join additional tags into JSON string
		// 	json = subsystemTagsToJSON(strings.Split(tags[commonTagsLen], ","))
		// } else {
		// 	// No additional tags
		// 	json = ""
		// }

		// fields line ex.:
		// 1451606400000000000,58,2,24,61,22,63,6,44,80,38
		metrics := strings.Split(row.fields, ",")

		// Count number of metrics processed
		ret += uint64(len(metrics) - 1) // 1-st field is timestamp, do not count it
		// metrics = (
		// 	1451606400000000000,
		// 	58,
		// )

		// Build string TimeStamp as '2006-01-02 15:04:05.999999 -0700'
		// convert time from 1451606400000000000 (int64 UNIX TIMESTAMP with nanoseconds)
		timestampNano, err := strconv.ParseInt(metrics[0], 10, 64)
		if err != nil {
			panic(err)
		}
		timeUTC := time.Unix(0, timestampNano)
		TimeUTCStr := timeUTC.Format("2006-01-02 15:04:05.999999 -0700")

		// use nil at 2-nd position as placeholder for tagKey
		r := make([]interface{}, 0, colLen)
		// First columns in table are
		// created_date
		// created_at
		// time
		// tags_id - would be nil for now
		// additional_tags
		tagsIdPosition = 3 // what is the position of the tags_id in the row - nil value
		r = append(r,
			timeUTC,    // created_date
			timeUTC,    // created_at
			TimeUTCStr, // time
			nil)        // tags_id
		// json)       // additional_tags

		if p.conf.InTableTag {
			r = append(r, tags[0]) // tags[0] = hostname
		}
		for _, v := range metrics[1:] {
			if v == "" {
				r = append(r, nil)
				continue
			}
			f64, err := strconv.ParseFloat(v, 64)
			if err != nil {
				panic(err)
			}
			r = append(r, f64)
		}

		dataRows = append(dataRows, r)
		tagRows = append(tagRows, tags)
	}

	// Check if any of these tags has yet to be inserted
	// New tags in this batch, need to be inserted
	newTags := make([][]string, 0, len(rows))
	p.csi.mutex.RLock()
	for _, tagRow := range tagRows {
		// tagRow contains what was called `tags` earlier - see one screen higher
		// tagRow[0] = hostname
		if _, ok := p.csi.m[tagRow[0]]; !ok {
			// Tags of this hostname are not listed as inserted - new tags line, add it for creation
			newTags = append(newTags, tagRow)
		}
	}
	p.csi.mutex.RUnlock()

	// Deal with new tags
	if len(newTags) > 0 {
		// We have new tags to insert
		p.csi.mutex.Lock()
		hostnameToTags := insertTags(p.conf, p.conn, len(p.csi.m), newTags, true)
		// Insert new tags into map as well
		for hostName, tagsId := range hostnameToTags {
			p.csi.m[hostName] = tagsId
		}
		p.csi.mutex.Unlock()
	}

	// Deal with tag ids for each data row
	p.csi.mutex.RLock()
	for i := range dataRows {
		// tagKey = hostname
		tagKey := tagRows[i][0]
		// Insert id of the tag (tags.id) for this host into tags_id position of the dataRows record
		// refers to
		// nil,		// tags_id

		dataRows[i][tagsIdPosition] = p.csi.m[tagKey]
	}
	p.csi.mutex.RUnlock()

	// Prepare column names
	cols := make([]string, 0, colLen)
	// First columns would be "created_date", "created_at", "time", "tags_id", "additional_tags"
	// Inspite of "additional_tags" being added the last one in CREATE TABLE stmt
	// it goes as a third one here - because we can move columns - they are named
	// and it is easier to keep variable coumns at the end of the list
	cols = append(cols, "created_date", "created_at", "time", "tags_id") //, "additional_tags")
	if p.conf.InTableTag {
		cols = append(cols, tableCols["tags"][0]) // hostname
	}
	cols = append(cols, tableCols[tableName]...)

	batch, err := p.conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))
	if err != nil {
		panic(err)
	}

	for _, r := range dataRows {
		// clickhouse-go doesn't auto convert types, so we do it manually via reflection
		for i, v := range r {
			t := reflect.TypeOf(v)
			if t == nil {
				continue
			}
			if v == "" {
				r[i] = nil
				continue
			}
			switch t.Kind() {
			case reflect.Float32:
				r[i] = float32(v.(float64))
			case reflect.Float64:
				r[i] = v.(float64)
			case reflect.Int32:
				r[i] = int32(v.(int64))
			case reflect.Int64:
				r[i] = v.(int64)
			case reflect.String:
				r[i] = v.(string)
			case reflect.Struct:
				if t.String() == "time.Time" {
					r[i] = v.(time.Time)
				}
			default:
				panic(fmt.Sprintf("unrecognized type %s with value %v of type %T",
					t.Kind(), v, v))
			}
		}

		if err := batch.Append(r...); err != nil {
			panic(err)
		}
	}

	if err := batch.Send(); err != nil {
		panic(err)
	}

	return ret
}

// insertTags fills tags table with values
func insertTags(_ *ClickhouseConfig, conn driver.Conn, startID int, rows [][]string, returnResults bool) map[string]int64 {
	// Map hostname to tags_id
	ret := make(map[string]int64)

	// reflect tags table structure which is
	// CREATE TABLE tags(
	//	 created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	//	 id,
	//   %s
	// ) engine=MergeTree(created_at, (%s), 8192)

	// build insert-multiple-rows INSERT statement like:
	// INSERT INTO tags (
	//   ... list of column names ...
	// ) VALUES
	// ( ... row 1 values ... ),
	// ( ... row 2 values ... ),
	// ...
	// ( ... row N values ... ),

	// Columns. Ex.:
	// hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO event")

	if err != nil {
		panic(err)
	}

	id := startID
	for _, row := range rows {
		// id of the new tag
		id++

		// unfortunately, it is not possible to pass a slice into variadic function of type interface
		// more details on the item:
		// https://blog.learngoprogramming.com/golang-variadic-funcs-how-to-patterns-369408f19085
		// Passing a slice to variadic param with an empty-interface
		var variadicArgs []interface{} = make([]interface{}, len(row)+1) // +1 here for additional 'id' column value
		// Place id at the beginning
		variadicArgs[0] = id
		// And all the rest of column values afterwards
		for i, value := range row {
			variadicArgs[i+1] = convertBasedOnType(tagColumnTypes[i], value)
		}

		// default DB values for created_at and created_date seem
		// not to be working with batch inserts,
		// so we add them manually
		now := time.Now().UTC()
		variadicArgs = append([]interface{}{now, now}, variadicArgs...)

		// TODO: sometimes, the map is nil and I don't know why
		// this is a workaround for now
		if variadicArgs[len(variadicArgs)-2] == nil {
			variadicArgs[len(variadicArgs)-2] = map[string]string{}
		}

		if err := batch.Append(variadicArgs...); err != nil {
			panic(err)
		}

		// Fill map hostname -> id
		if returnResults {
			// Map hostname -> tags_id
			ret[row[0]] = int64(id)
		}
	}

	if err := batch.Send(); err != nil {
		panic(err)
	}

	if returnResults {
		return ret
	}

	return nil
}

func jsonToMap(jsonStr string) map[string]string {
	result := make(map[string]string)
	json.Unmarshal([]byte(jsonStr), &result)
	return result
}

func convertBasedOnType(serializedType, value string) interface{} {
	if value == "" {
		return nil
	}

	// starts with 'map{' -> Map
	if strings.HasPrefix(value, "map{") {
		// remove the "map" prefix
		value = value[3:]
		// replace ";" with ","
		value = strings.ReplaceAll(value, ";", ",")
		m := jsonToMap(value)
		return m
	}

	// starts with 'json{' -> JSON
	if strings.HasPrefix(value, "json{") {
		// remove the "json" prefix
		value = value[4:]
		// replace ";" with ","
		value = strings.ReplaceAll(value, ";", ",")
		return value
	}

	switch serializedType {
	case "string":
		return value
	case "float32":
		f, err := strconv.ParseFloat(value, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float32", value))
		}
		return float32(f)
	case "float64":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float64", value))
		}
		return f
	case "int64":
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return i
	case "int32":
		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return int32(i)
	default:
		panic(fmt.Sprintf("unrecognized serializedType %s with value %s of type %T",
			serializedType, value, value))
	}
}

// subsystemTagsToJSON converts equations as
// a=b
// c=d
// into JSON STRING '{"a": "b", "c": "d"}'
func subsystemTagsToJSON(tags []string) string {
	json := "{"
	for i, t := range tags {
		args := strings.Split(t, "=")
		if i > 0 {
			json += ","
		}
		json += fmt.Sprintf("\"%s\": \"%s\"", args[0], args[1])
	}
	json += "}"
	return json
}
