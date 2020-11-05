package atlasengine

import (
	"time"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/aggregators"
)

type Aggregate struct {
	name   string
	tags   map[string]string
	fields Statistics
}

type AggregateInstance struct {
	name   string
	tags   map[string]string
	fields StatisticsInstance
}

type Statistics struct {
	finishedErrorFree int
	finishedError     int
	running           int
}

type StatisticsInstance struct {
	// Just push if completed!!
	startTime int64
	stopTime  int64
	duration  int64
	withError bool
}

// AtlasEngine an aggregation plugin
type AtlasEngine struct {
	processModelCache         map[string]Aggregate
	processModelInstanceCache map[string]AggregateInstance
	flowNodeCache             map[string]Aggregate
	flowNodeInstanceCache     map[string]AggregateInstance
	// Fields []string
}

// NewAtlasEngine create a new aggregation plugin which counts the occurrences
// of different tag combinations and and emits the count.
func NewAtlasEngine() telegraf.Aggregator {
	ae := &AtlasEngine{}
	ae.resetCache()
	return ae
}

var sampleConfig = `
  ## General Aggregator Arguments:
  ## The period on which to flush & clear the aggregator.
  period = "30s"`

// SampleConfig generates a sample config for the AtlasEngine plugin
func (atlasEngine *AtlasEngine) SampleConfig() string {
	return sampleConfig
}

// Description returns the description of the AtlasEngine plugin
func (atlasEngine *AtlasEngine) Description() string {
	return "Aggregate events from the atlas-engine to generate process-metrics."
}

func isProcessStarted(eventType string) bool {
	return eventType == "OnProcessStarted"
}

func isProcessStopped(eventType string) bool {
	switch eventType {
	case
		"onProcessError",
		"OnProcessFinished",
		"OnProcessTerminated":
		return true
	}
	return false
}

func isProcessModelError(eventType string) bool {
	return eventType == "onProcessError"
}

func isFlowNodeStarted(eventType string) bool {
	return eventType == "OnFlowNodeEntered"
}

func isFlowNodeStopped(eventType string) bool {
	switch eventType {
	case
		"OnFlowNodeError",
		"OnFlowNodeExited",
		"OnFlowNodeTerminated":
		return true
	}
	return false
}

func isFlowNodeError(eventType string) bool {
	return eventType == "OnFlowNodeError"
}

func (atlasEngine *AtlasEngine) initProcessModelCache(metric telegraf.Metric) {

	processModelId, _ := metric.GetTag("processModelId")
	processInstanceId, _ := metric.GetTag("processInstanceId")

	// hit an uncached processModelId, create caches for first time
	if _, ok := atlasEngine.processModelCache[processModelId]; !ok {
		aggregate := Aggregate{
			name: metric.Name(),
			tags: map[string]string{"processModelId": processModelId},
			fields: Statistics{
				finishedError:     0,
				finishedErrorFree: 0,
				running:           0,
			},
		}
		atlasEngine.processModelCache[processModelId] = aggregate
	}

	// hit an uncached processInstanceId
	if _, ok := atlasEngine.processModelInstanceCache[processInstanceId]; !ok {
		aggregateInstance := AggregateInstance{
			name: metric.Name(),
			tags: map[string]string{"processModelId": processModelId, "processInstanceId": processInstanceId},
			fields: StatisticsInstance{
				startTime: 0,
				stopTime:  0,
				duration:  0,
				withError: true,
			},
		}
		atlasEngine.processModelInstanceCache[processInstanceId] = aggregateInstance
	}
}

func (atlasEngine *AtlasEngine) initFlowNodeCache(metric telegraf.Metric) {

	flowNodeId, ok := metric.GetTag("flowNodeId")
	flowNodeInstanceId, _ := metric.GetTag("flowNodeInstanceId")
  flowNodeType, _ := metric.GetTag("flowNodeType")

  if !ok{
    return
  }

	// hit an uncached flowNodeId, create caches for first time
	if _, ok := atlasEngine.flowNodeCache[flowNodeId]; !ok {
		aggregate := Aggregate{
			name: metric.Name(),
			tags: map[string]string{"flowNodeId": flowNodeId, "flowNodeType": flowNodeType},
			fields: Statistics{
				finishedError:     0,
				finishedErrorFree: 0,
				running:           0,
			},
		}
		atlasEngine.flowNodeCache[flowNodeId] = aggregate
	}

	// hit an uncached processInstanceId
	if _, ok := atlasEngine.flowNodeInstanceCache[flowNodeInstanceId]; !ok {
		aggregateInstance := AggregateInstance{
			name: metric.Name(),
			tags: map[string]string{"flowNodeId": flowNodeId, "flowNodeType": flowNodeType, "flowNodeInstanceId": flowNodeInstanceId},
			fields: StatisticsInstance{
				startTime: 0,
				stopTime:  0,
				duration:  0,
			},
		}
		atlasEngine.flowNodeInstanceCache[flowNodeInstanceId] = aggregateInstance
	}
}

func (atlasEngine *AtlasEngine) handleProcessModelLogic(metric telegraf.Metric) {

	eventType, _ := metric.GetTag("eventType")
	processModelId, _ := metric.GetTag("processModelId")
	processInstanceId, _ := metric.GetTag("processInstanceId")

	// handel started Processes
	if isProcessStarted(eventType) {

		temp := atlasEngine.processModelCache[processModelId]
		temp.fields.running++
		atlasEngine.processModelCache[processModelId] = temp

		tempInstance := atlasEngine.processModelInstanceCache[processInstanceId]
		tempInstance.fields.startTime = time.Now().UnixNano()
		atlasEngine.processModelInstanceCache[processInstanceId] = tempInstance

	}

	// handel stopped Processes
	if isProcessStopped(eventType) {

		temp := atlasEngine.processModelCache[processModelId]
		tempInstance := atlasEngine.processModelInstanceCache[processInstanceId]

		if isProcessModelError(eventType) {
			temp.fields.finishedError++
			tempInstance.fields.withError = true
		} else {
			temp.fields.finishedErrorFree++
			tempInstance.fields.withError = false
		}

		if temp.fields.running > 0 {
			temp.fields.running--
		}

		tempInstance.fields.stopTime = time.Now().UnixNano()
		tempInstance.fields.duration = time.Now().UnixNano() - tempInstance.fields.startTime

		atlasEngine.processModelCache[processModelId] = temp
		atlasEngine.processModelInstanceCache[processInstanceId] = tempInstance
	}
}

func (atlasEngine *AtlasEngine) handleFlowNodeLogic(metric telegraf.Metric) {


  flowNodeId, ok := metric.GetTag("flowNodeId")
	flowNodeInstanceId, _ := metric.GetTag("flowNodeInstanceId")
  eventType, _ := metric.GetTag("eventType")

  if !ok{
    return
  }


	// handel started FlowNode
	if isFlowNodeStarted(eventType) {

		temp := atlasEngine.flowNodeCache[flowNodeId]
		temp.fields.running++
		atlasEngine.flowNodeCache[flowNodeId] = temp

		tempInstance := atlasEngine.flowNodeInstanceCache[flowNodeInstanceId]
		tempInstance.fields.startTime = time.Now().UnixNano()
		atlasEngine.flowNodeInstanceCache[flowNodeInstanceId] = tempInstance

	}

	// handel finished FlowNode
	if isFlowNodeStopped(eventType) {
		temp := atlasEngine.flowNodeCache[flowNodeId]
		tempInstance := atlasEngine.flowNodeInstanceCache[flowNodeInstanceId]

		if isFlowNodeError(eventType) {
			temp.fields.finishedError++
			tempInstance.fields.withError = true
		} else {
			temp.fields.finishedErrorFree++
			tempInstance.fields.withError = false
		}

		if temp.fields.running > 0 {
			temp.fields.running--
		}

		tempInstance.fields.stopTime = time.Now().UnixNano()
		tempInstance.fields.duration = time.Now().UnixNano() - tempInstance.fields.startTime

		atlasEngine.flowNodeCache[flowNodeId] = temp
		atlasEngine.flowNodeInstanceCache[flowNodeInstanceId] = tempInstance
	}
}

// Add is run on every metric which passes the plugin
func (atlasEngine *AtlasEngine) Add(metric telegraf.Metric) {

	atlasEngine.initProcessModelCache(metric)
	atlasEngine.initFlowNodeCache(metric)

	atlasEngine.handleProcessModelLogic(metric)
	atlasEngine.handleFlowNodeLogic(metric)
}

// Push processModel-/ flowNode- Metrics
func (atlasEngine *AtlasEngine) Push(accumulator telegraf.Accumulator) {

	for _, aggregate := range atlasEngine.processModelCache {

		fieldsCounter := map[string]interface{}{}
		fieldsCounter["processModel_finishedError"] = aggregate.fields.finishedError
		fieldsCounter["processModel_finishedErrorFree"] = aggregate.fields.finishedErrorFree
    accumulator.AddCounter(aggregate.name, fieldsCounter, aggregate.tags)

		fieldsGauge := map[string]interface{}{}
		fieldsGauge["processModel_running"] = aggregate.fields.running
		accumulator.AddGauge(aggregate.name, fieldsGauge, aggregate.tags)
	}

	for processInstanceId, aggregateInstance := range atlasEngine.processModelInstanceCache {

		// skip pushing metrick because processInstance is not finished yet
		if aggregateInstance.fields.stopTime == 0 {
			continue
		}

		fieldsGauge := map[string]interface{}{}
		fieldsGauge["processModelInstance_startTime"] = aggregateInstance.fields.startTime
		fieldsGauge["processModelInstance_stopTime"] = aggregateInstance.fields.stopTime
		fieldsGauge["processModelInstance_duration"] = aggregateInstance.fields.duration

		delete(atlasEngine.processModelInstanceCache, processInstanceId)
		accumulator.AddGauge(aggregateInstance.name, fieldsGauge, aggregateInstance.tags)
	}

	for _, aggregate := range atlasEngine.flowNodeCache {

		fieldsCounter := map[string]interface{}{}
		fieldsCounter["flowNode_finishedError"] = aggregate.fields.finishedError
		fieldsCounter["flowNode_finishedErrorFree"] = aggregate.fields.finishedErrorFree
    accumulator.AddCounter(aggregate.name, fieldsCounter, aggregate.tags)

		fieldsGauge := map[string]interface{}{}
		fieldsGauge["flowNode_running"] = aggregate.fields.running
		accumulator.AddGauge(aggregate.name, fieldsGauge, aggregate.tags)
	}

	for flowNodeInstanceId, aggregateInstance := range atlasEngine.flowNodeInstanceCache {

		// skip pushing metrick because processInstance is not finished yet
		if aggregateInstance.fields.stopTime == 0 {
			continue
		}

		fieldsGauge := map[string]interface{}{}
		fieldsGauge["flowNodeInstance_startTime"] = aggregateInstance.fields.startTime
		fieldsGauge["flowNodeInstance_stopTime"] = aggregateInstance.fields.stopTime
		fieldsGauge["flowNodeInstance_duration"] = aggregateInstance.fields.duration

		delete(atlasEngine.flowNodeInstanceCache, flowNodeInstanceId)
		accumulator.AddGauge(aggregateInstance.name, fieldsGauge, aggregateInstance.tags)

	}
}

// Reset does nothing
func (atlasEngine *AtlasEngine) Reset() {

}

func (atlasEngine *AtlasEngine) resetCache() {
	atlasEngine.processModelCache = make(map[string]Aggregate)
	atlasEngine.processModelInstanceCache = make(map[string]AggregateInstance)
	atlasEngine.flowNodeCache = make(map[string]Aggregate)
	atlasEngine.flowNodeInstanceCache = make(map[string]AggregateInstance)
}

func init() {
	aggregators.Add("atlasengine", func() telegraf.Aggregator {
		return NewAtlasEngine()
	})
}
