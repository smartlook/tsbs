package dea

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/data/usecases/iot"
	"github.com/timescale/tsbs/pkg/publicid"
)

const (
	UrlProperty            = "url"
	EmailProperty          = "email"
	NameProperty           = "name"
	PhoneProperty          = "phone"
	FavoriteNumberProperty = "age"
)

const (
	NavigationEvent = "navigation"
	ClickEvent      = "click"
	CheckoutEvent   = "cart_checkout"
	TypingEvent     = "typing"
)

var (
	AvailableEvents            = []string{NavigationEvent, ClickEvent, CheckoutEvent, TypingEvent}
	AvailableStrProperties     = []string{UrlProperty, EmailProperty, NameProperty, PhoneProperty}
	AvailableStrPropertyValues = map[string][]string{
		UrlProperty:   {"https://www.seznam.cz"},
		EmailProperty: {"hello@world.com"},
		NameProperty:  {"john doe"},
		PhoneProperty: {"+420602303222"},
	}
)

var (
	OtelChoices = map[string][]string{
		UrlProperty:   {"https://www.seznam.cz"},
		EmailProperty: {"hello@world.com"},
		NameProperty:  {"john doe"},
		PhoneProperty: {"+420602303222"},
		// "telemetry.auto.version":      {"1.23.0", "1.23.1", "1.23.2", "1.23.3", "1.23.4"},
		// "os.description":              {"Linux 5.10.104-linuxkit", "Linux 5.10.105-linuxkit", "Linux 5.10.106-linuxkit", "Linux 5.10.107-linuxkit", "Linux 5.10.108-linuxkit"},
		// "process.runtime.description": {"Eclipse Adoptium OpenJDK 64-Bit Server VM 17.0.6+10", "Eclipse Adoptium OpenJDK 64-Bit Server VM 17.0.7+10", "Eclipse Adoptium OpenJDK 64-Bit Server VM 17.0.8+10", "Eclipse Adoptium OpenJDK 64-Bit Server VM 17.0.9+10", "Eclipse Adoptium OpenJDK 64-Bit Server VM 17.0.10+10"},
		// "service.name":                {"adservice", "bbservice", "ccservice", "ddservice", "eeservice"},
		// "service.namespace":           {"opentelemetry-demo", "opentelemetry-demo", "opentelemetry-demo", "opentelemetry-demo", "opentelemetry-demo"},
		// "telemetry.sdk.version":       {"1.23.0", "1.23.1", "1.23.2", "1.23.3", "1.23.4"},
		// "process.runtime.version":     {"17.0.6+10", "17.0.7+10", "17.0.8+10", "17.0.9+10", "17.0.10+10"},
		// "telemetry.sdk.name":          {"opentelemetry", "opentelemetry", "opentelemetry", "opentelemetry", "opentelemetry"},
		// "host.arch":                   {"aarch64", "aarch64", "aarch64", "aarch64", "aarch64"},
		// "host.name":                   {"c97f4b793890", "c97f4b793891", "c97f4b793892", "c97f4b793893", "c97f4b793894"},
		// "process.executable.path": {
		// 	"/opt/1/java/openjdk/bin/java",
		// 	"/opt/2/java/openjdk/bin/java",
		// 	"/opt/3/java/openjdk/bin/java",
		// 	"/opt/4/java/openjdk/bin/java",
		// 	"/opt/5/java/openjdk/bin/java",
		// },
		// "process.pid":          {"1", "2", "3", "4", "5"},
		// "process.runtime.name": {"OpenJDK Runtime Environment", "Oracle JDK Runtime Environment", "Eclipse Adoptium OpenJDK Runtime Environment", "Amazon Corretto JDK Runtime Environment", "IBM JDK Runtime Environment"},
		// "container.id": {
		// 	"c97f4b7938901101550efbda3c250414cee6ba9bfb4769dc7fe156cb2311735e",
		// 	"c97f4b7938911101550efbda3c250414cee6ba9bfb4769dc7fe156cb2311735e",
		// 	"c97f4b7938921101550efbda3c250414cee6ba9bfb4769dc7fe156cb2311735e",
		// },
		// "os.type": {"linux", "macos", "windows"},
		// "process.command_line": {
		// 	"/opt/1/java/openjdk/bin/java -javaagent:/usr/src/app/opentelemetry-javaagent.jar",
		// 	"/opt/2/java/openjdk/bin/java -javaagent:/usr/src/app/opentelemetry-javaagent.jar",
		// 	"/opt/3/java/openjdk/bin/java -javaagent:/usr/src/app/opentelemetry-javaagent.jar",
		// },
		// "telemetry.sdk.language": {"java", "go", "python", "javascript", "csharp"},
	}

	OtelChoicesKeys = func() []string {
		keys := make([]string, 0, len(OtelChoices))
		for k := range OtelChoices {
			keys = append(keys, k)
		}
		return keys
	}()
)

func generateArrayOfIds(n int, prefix string) []string {
	var users = make([]string, n)

	for i := range n {
		users[i] = publicid.MustWithPrefix(prefix)
	}

	return users
}

var knownUsers = generateArrayOfIds(200000, "u")
var knownTenants = generateArrayOfIds(100, "t")

type Event struct {
	simulatedMeasurements []common.SimulatedMeasurement
	tags                  []common.Tag
}

// TickAll advances all Distributions of an Event.
func (e *Event) TickAll(d time.Duration) {
	for i := range e.simulatedMeasurements {
		e.simulatedMeasurements[i].Tick(d)
	}
}

// Measurements returns the event measurements.
func (e Event) Measurements() []common.SimulatedMeasurement {
	return e.simulatedMeasurements
}

// Tags returns the event tags.
func (e Event) Tags() []common.Tag {
	return e.tags
}

func newEventMeasurements(start time.Time) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		iot.NewReadingsMeasurement(start),
		iot.NewDiagnosticsMeasurement(start),
	}
}

// NewEvent returns a new simulated event.
func NewEvent(i int, start time.Time) common.Generator {
	event := newEventWithMeasurementGenerator(i, start, newEventMeasurements)
	return &event
}

// getRandomMap generates a random one-level deep map with a given number of keys
func getRandomMap(numOfKeys int) map[string]string {
	var m = make(map[string]string, numOfKeys)
	for i := 0; i < numOfKeys; i++ {
		m[gofakeit.Word()] = gofakeit.Word()
	}
	return m
}

// getMapFromChoices generates a map with a random number of keys from a given set of choices
func getMapFromChoices(keyChoices []string, valueChoices []string) map[string]string {
	var m = make(map[string]string, len(keyChoices))
	for i, k := range keyChoices {
		m[k] = valueChoices[i]
	}
	return m
}

// getMapOtelChoices generates a map with a random number of keys from a given set of choices
func getMapOtelChoices() map[string]string {
	var m = make(map[string]string, len(OtelChoicesKeys))
	for _, k := range OtelChoicesKeys {
		m[k] = gofakeit.RandomString(OtelChoices[k])
	}
	return m
}

func newEventWithMeasurementGenerator(_ int, start time.Time, generator func(time.Time) []common.SimulatedMeasurement) Event {
	sm := generator(start)

	h := Event{
		tags: []common.Tag{
			{
				Key: []byte("event_id"),
				Value: func() interface{} {
					return publicid.MustWithPrefix("e")
				},
			},
			{
				Key: []byte("user_id"),
				Value: func() interface{} {
					return gofakeit.RandomString(knownUsers)
				},
			},
			{
				Key: []byte("session_id"),
				Value: func() interface{} {
					return publicid.MustWithPrefix("s")
				},
			},
			{
				Key: []byte("tenant_id"),
				Value: func() interface{} {
					return gofakeit.RandomString(knownTenants)
				},
			},
			{
				Key: []byte("timestamp"),
				Value: func() interface{} {
					return time.Now().UTC().Unix()
				},
			},
			{
				Key: []byte("name"),
				Value: func() interface{} {
					return gofakeit.RandomString(AvailableEvents)
				},
			},
			{
				Key: []byte("properties_map"),
				Value: func() interface{} {
					m := getMapOtelChoices() // getRandomMap(gofakeit.Number(5, 30))
					j, err := json.Marshal(m)
					if err != nil {
						panic(err)
					}
					// Replace separator "," with ";"
					j = []byte(strings.ReplaceAll(string(j), ",", ";"))
					return "map" + string(j)
				},
			},
			{
				Key: []byte("properties_json"),
				Value: func() interface{} {
					m := getMapOtelChoices() // getRandomMap(gofakeit.Number(5, 30))
					j, err := json.Marshal(m)
					if err != nil {
						log.Fatal(err)
					}
					// Replace separator "," with ";"
					j = []byte(strings.ReplaceAll(string(j), ",", ";"))
					return "json" + string(j)
				},
			},
		},
		simulatedMeasurements: sm,
	}

	return h
}
