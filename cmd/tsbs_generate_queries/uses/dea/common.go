package dea

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/pkg/query"
)

const (
	TableName = "events"

	LabelNestedWhere    = "nested-where"
	LabelEventHistogram = "event-histogram"
	LabelFunnel         = "funnel"
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

// Core is the common component of all generators for all systems.
type Core struct {
	*common.Core
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	c, err := common.NewCore(start, end, scale)
	return &Core{Core: c}, err
}

type NestedWhereFiller interface {
	NestedWhere(qi query.Query)
}

type EventHistogramFiller interface {
	EventHistogram(query.Query)
}

type FunnelFiller interface {
	Funnel(qi query.Query, inOrder bool, minSteps int, maxSteps int)
}

func (d *Core) GetRandomProperties(n int, properties []string) ([]string, error) {
	if n < 1 {
		return nil, fmt.Errorf("number of properties cannot be < 1; got %d", n)
	}
	if n > len(properties) {
		return nil, fmt.Errorf("number of properties (%d) larger than total properties (%d)", n, len(properties))
	}

	randomNumbers, err := common.GetRandomSubsetPerm(n, len(properties))
	if err != nil {
		return nil, err
	}

	selectedProperties := make([]string, n)
	for i, n := range randomNumbers {
		selectedProperties[i] = properties[n]
	}

	return selectedProperties, nil
}

func (d *Core) GetRandomPropertyValue(property string) (string, error) {
	if values, ok := AvailableStrPropertyValues[property]; ok {
		return values[rand.Intn(len(values))], nil
	} else {
		return "", fmt.Errorf("no values for %s", property)
	}
}

func (d *Core) GetRandomEvent() string {
	return AvailableEvents[rand.Intn(len(AvailableEvents))]
}
