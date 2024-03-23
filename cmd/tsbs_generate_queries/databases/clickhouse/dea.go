package clickhouse

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/dea"
	"github.com/timescale/tsbs/pkg/query"
)

// Devops produces ClickHouse-specific queries for all the devops query types.
type DEA struct {
	*dea.Core
	*BaseGenerator
}

const (
	Json string = "json"
	Map  string = "map"
)

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func (d *DEA) getJSONProperty(key string) string {
	return fmt.Sprintf("simpleJSONExtractRaw(propertiesJson, '%s')", key)
}

func (d *DEA) getMapProperty(key string) string {
	return fmt.Sprintf("propertiesMap['%s']", key)
}

func (d *DEA) getProperty(key string) string {
	switch d.PropertyAccessMode {
	case Json:
		return d.getJSONProperty(key)
	case Map:
		return d.getMapProperty(key)
	default:
		panic(fmt.Sprintf("unknown access mode %s", d.PropertyAccessMode))
	}
}

func (d *DEA) getPropertyAlias(property string) string {
	return fmt.Sprintf("a_%s", property)
}

func (d *DEA) getAliasedProperties(keys []string) []string {
	aliasedProps := make([]string, len(keys))

	for i := range keys {
		aliasedProps[i] = fmt.Sprintf("%s as %s", d.getProperty(keys[i]), d.getPropertyAlias(keys[i]))
	}

	return aliasedProps
}

func (d *DEA) getFunnelStepSelectStatements(nSteps int) []string {
	var statements []string

	for i := range nSteps {
		statements = append(statements, fmt.Sprintf("countIf(steps = %d) AS step_%d", i+1, i+1))
	}

	return statements
}

/*
IF(name = 'navigation' AND simpleJSONExtractString(properties, 'url') LIKE 'app.smartlook.com/sign/up%', 1, 0) AS step_0,
IF(step_0 = 1, timestamp, NULL) AS latest_0,
IF(name = 'signup_step_1', 1, 0) AS step_1,
IF(step_1 = 1, timestamp, NULL) AS latest_1,
IF(name = 'signup_step_2', 1, 0) AS step_2,
IF(step_2 = 1, timestamp, NULL) AS latest_2
*/
func (d *DEA) getFunnelFiltersStatements(nSteps int) []string {
	var statements []string

	for i := range nSteps {
		stepEvent := d.GetRandomEvent()
		eventProperty := Must(d.GetRandomProperties(1, dea.AvailableStrProperties))[0]
		eventValue := Must(d.GetRandomPropertyValue(eventProperty))

		statements = append(statements, fmt.Sprintf("IF (name = '%s' AND %s LIKE '%s', 1, 0)",
			stepEvent,
			eventProperty,
			eventValue))
		statements = append(statements, fmt.Sprintf("IF (step_%d = 1, timestamp, NULL) as latest_%d",
			i, i))
	}

	return statements
}

/*
step_0 = 1
OR step_1 = 1
OR step_2 = 1
*/
func (d *DEA) getFunnelConversionStatement(nSteps int) string {
	var statements []string

	for i := range nSteps - 1 {
		statements = append(statements, fmt.Sprintf("step_%d = 1", i))
	}

	return strings.Join(statements, " OR ")
}

func (d *DEA) getCustomStrictFunnelSQL(nSteps int) string {
	interval := d.Interval.MustRandWindow(time.Hour * 24) // TODO: Update to some other interval

	sql := fmt.Sprintf(`
	SELECT %s
	FROM (
	SELECT user_id,
			steps
	FROM (
		SELECT user_id,
			steps,
			max(steps) OVER (PARTITION BY user_id) AS max_steps,
		FROM (
			SELECT *,
				IF(
					latest_0 <= latest_1
					AND latest_1 <= latest_0 + INTERVAL 14 DAY
					AND latest_1 <= latest_2
					AND latest_2 <= latest_1 + INTERVAL 14 DAY,
					3,
					IF(
						latest_0 <= latest_1
						AND latest_1 <= latest_0 + INTERVAL 14 DAY,
						2,
						1
					)
				) AS steps,
				IF(
					isNotNull(latest_1)
					AND latest_1 <= latest_0 + INTERVAL 14 DAY,
					dateDiff(
						'second',
						toDateTime(latest_0),
						toDateTime(latest_1)
					),
					NULL
				) AS step_1_conversion_time,
				IF(
				isNotNull(latest_2)
				AND latest_2 <= latest_1 + INTERVAL 14 DAY,
				dateDiff(
					'second',
					toDateTime(latest_1),
					toDateTime(latest_2)
				),
				NULL
				) AS step_2_conversion_time
		FROM (
			SELECT user_id,
				eventDate,
				step_0,
				min(latest_0) OVER (
					PARTITION BY user_id
					ORDER BY eventDate DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING
				) latest_0,
				step_1,
				min(latest_1) OVER (
					PARTITION BY user_id
					ORDER BY eventDate DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING
				) latest_1,
				step_2,
				min(latest_2) OVER (
					PARTITION BY user_id
					ORDER BY eventDate DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING
				) latest_2
			FROM (
			SELECT user_id,
					timestamp AS eventDate,
					name,
					%s
			FROM (
				SELECT user_id, id, properties, name, timestamp
				FROM %s e
				WHERE timestamp >= '%s' AND timestamp <= '%s'
			) WHERE (
				%s
			)
			)
		)
		WHERE step_0 = 1
		)
	)
	GROUP BY user_id, steps
	HAVING steps = max_steps
	)
	`, strings.Join(d.getFunnelStepSelectStatements(nSteps), ",\n"),
		strings.Join(d.getFunnelFiltersStatements(nSteps), ",\n"),
		dea.TableName,
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat),
		d.getFunnelConversionStatement(nSteps))

	return sql
}

func (d *DEA) getInternalStrictFunnelSQL(nSteps int) string {
	var steps []string
	observedEvents := make(map[string]bool)

	for range nSteps {
		event := d.GetRandomEvent()
		observedEvents[event] = true

		property := Must(d.GetRandomProperties(1, dea.AvailableStrProperties))[0]
		propertyValue := Must(d.GetRandomPropertyValue(property))

		stepStatement := fmt.Sprintf("name = '%s' AND %s = '%s'",
			event, property, propertyValue)
		steps = append(steps, stepStatement)
	}

	var observedEventsStatements []string
	for event := range observedEvents {
		observedEventsStatements = append(observedEventsStatements, fmt.Sprintf("'%s'", event))
	}

	sql := fmt.Sprintf(`SELECT
    level,
    count() AS c
	FROM (
		SELECT
			user_id,
			windowFunnel(6048000000000000)(timestamp, 
				%s
			) AS level
		FROM 
		 %s
		WHERE (
			name IN (%s)
		)
		GROUP BY user_id 
	)
	GROUP BY level
	ORDER BY level ASC;`,
		strings.Join(steps, ",\n"),
		dea.TableName,
		strings.Join(observedEventsStatements, ", "),
	)

	return sql
}

func (d *DEA) NestedWhere(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour * 24) // TODO: Update to some other interval
	randomProperties, err := d.GetRandomProperties(4, dea.AvailableStrProperties)
	panicIfErr(err)

	selectClauses := strings.Join(d.getAliasedProperties(randomProperties), ", ")

	whereClauses := fmt.Sprintf("%s = '%s' AND %s != '' AND (%s LIKE '%%%s%%' OR %s LIKE '%%%s') "+
		"AND (createdAt >= '%s') AND (createdAt <= '%s')",
		d.getPropertyAlias(randomProperties[0]),
		Must(d.GetRandomPropertyValue(randomProperties[0])),
		d.getPropertyAlias(randomProperties[1]),
		d.getPropertyAlias(randomProperties[2]),
		Must(d.GetRandomPropertyValue(randomProperties[2]))[2:6],
		d.getPropertyAlias(randomProperties[3]),
		Must(d.GetRandomPropertyValue(randomProperties[3]))[2:],
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat))

	sql := fmt.Sprintf(`
		SELECT toStartOfHour(created_at) AS hour, %s FROM %s WHERE %s
	`, selectClauses, dea.TableName, whereClauses)

	humanLabel := "ClickHouse nested and dynamic"
	humanDesc := fmt.Sprintf("%s: nested where query with dynamic data access", humanLabel)
	d.fillInQuery(qi, humanLabel, humanDesc, dea.TableName, sql)
}

func (d *DEA) EventHistogram(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour * 24) // TODO: Update to some other interval

	sql := fmt.Sprintf(`
	SELECT
		toDate(timestamp, 'Europe/Prague') as day,
		uniq(user_id) as visitors,
		count() as cnt
	FROM %s
	WHERE
		name LIKE '%s'
		AND timestamp >= now() - INTERVAL 30 DAY
	GROUP BY
		day
	ORDER BY
    	day WITH FILL
    FROM toDate(%s)
    TO toDate(%s)
	`,
		dea.TableName,
		d.GetRandomEvent(),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat))

	d.fillInQuery(qi, "ClickHouse Event histogram", "Clickhouse Event histogram", dea.TableName, sql)
}

func (d *DEA) Funnel(qi query.Query, inOrder bool, minSteps, maxSteps int) {
	if maxSteps < minSteps {
		panic(fmt.Errorf("maximum steps (%d) is less than minimum steps (%d)", maxSteps, minSteps))
	}

	nSteps := rand.Intn(maxSteps-minSteps) + minSteps + 1
	// selectStatement :=

	sql := d.getInternalStrictFunnelSQL(nSteps)

	d.fillInQuery(qi, "ClickHouse Event histogram", "Clickhouse Event histogram", dea.TableName, sql)
}
