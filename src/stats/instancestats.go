package stats

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type DefaultIMetrics struct{}

func (d DefaultIMetrics) Get() []string {
	return []string{
		"My Phase 1 Proposals",
		"My Phase 2 Proposals",
		"My Phase 1 Preempted",
		"My Phase 2 Preempted",
		"Phase 1 Timeouts",
		"Phase 2 Timeouts",
		"Choosing Round Attempts",
		"Noop Proposed",
		"Client Value Proposed",
		"Previous Value Proposed",
		"I Chose",
		"Timeout triggered Phase 1",
		"Timeout triggered Phase 2",
	}
}

type InstanceID struct {
	Log int32
	Seq int32
}

//stats to collect
// noop proposed
// value proposed
// how many client values proposed
// conflicts us had first phase
// conflicts us had second phase
// how many attempts to choose in final
// I began proposing at?
//

type timeTuple struct {
	Open    time.Time
	Commit  time.Time
	Execute time.Time
}

type commitExecutionComparator struct {
	cmdsTimes map[InstanceID]timeTuple
}

func commitExecutionComparatorNew() *commitExecutionComparator {
	return &commitExecutionComparator{
		cmdsTimes: make(map[InstanceID]timeTuple),
	}
}

func (c *commitExecutionComparator) recordOpen(id InstanceID, time time.Time) {
	timeTup, _ := c.cmdsTimes[id]
	if !timeTup.Open.IsZero() {
		panic("cannot overwrite open time")
	}
	c.cmdsTimes[id] = timeTuple{Open: time}
}

func (c *commitExecutionComparator) recordCommit(id InstanceID, time time.Time) {
	timeTup, _ := c.cmdsTimes[id]
	if !timeTup.Commit.IsZero() {
		panic("cannot overwrite commit time")
	}
	c.cmdsTimes[id] = timeTuple{
		Open:   timeTup.Open,
		Commit: time,
	}
}

func (c *commitExecutionComparator) recordExecution(id InstanceID, time time.Time) {
	timeTup, exists := c.cmdsTimes[id]
	if !exists {
		panic("somehow recording execution when not first recorded commiting")
	}
	if !timeTup.Execute.IsZero() {
		panic("cannot overwrite execution time")
	}
	c.cmdsTimes[id] = timeTuple{
		Open:    timeTup.Open,
		Commit:  timeTup.Commit,
		Execute: time,
	}
	//	c.outputInstanceTimes(id)
}

// todo give thing that can create matrix
// need to track from when opened
// cause can change over time
func (c *commitExecutionComparator) outputInstanceTimes(id InstanceID) string {
	timeTup, exists := c.cmdsTimes[id]
	if !exists {
		panic("cannot RecordOutcome a command with no record of commit or execution")
	}
	cmtDiff := timeTup.Commit.Sub(timeTup.Open)
	execDiff := timeTup.Execute.Sub(timeTup.Commit)
	var timeOpenOut int64 = 0
	if !timeTup.Open.IsZero() {
		timeOpenOut = timeTup.Open.UnixNano()
	}
	return fmt.Sprintf("%d, %d, %d, %d, %d", timeOpenOut, timeTup.Commit.UnixNano(), timeTup.Execute.UnixNano(), cmtDiff.Microseconds(), execDiff.Microseconds())
}

func (c *commitExecutionComparator) getOutputFields() string {
	return "Open Time, Commit Time, Execute Time, Open-Commit Latency, Commit-Execute Latency"
}

type MultiStageConditionStatConstructor struct {
}

// fast qrm phase one --- y \/ n
// fast qrm phase two --- y '/ n
// slow qrm phase one --- y \/ n
// slow qrm phase two --- y \/ n

type MultiStartMultiOutStatConstructor struct {
	Name             string
	Starts           []string
	Outcomes         map[string][]string
	NegativeOutcomes map[string]string
	AssumeNegNoEnd   bool
}

func (stat *MultiStartMultiOutStatConstructor) Construct() *MutliStartMultiOutcomeStat {
	startsRec := make(map[string]struct{})
	outcomesRec := make(map[string]*MultiOutcomeStat)
	for _, s := range stat.Starts {
		startsRec[s] = struct{}{}
		outcomesMap := make(map[string]int)
		for _, sOutcome := range stat.Outcomes[s] {
			outcomesMap[sOutcome] = 0
		}
		outcomesRec[s] = &MultiOutcomeStat{
			started:    0,
			outcomes:   outcomesMap,
			negative:   stat.NegativeOutcomes[s],
			printOrder: stat.Outcomes[s],
		}
	}

	return &MutliStartMultiOutcomeStat{
		name:                   stat.Name,
		Outcomes:               outcomesRec,
		startsRecord:           startsRec,
		printOrder:             stat.Starts,
		negativeOutcomeOnNoEnd: stat.AssumeNegNoEnd,
		curStart:               "",
		started:                false,
	}
}

// if we start a stat it might end or might not. Need to sum up all stats that do not end properly
type MutliStartMultiOutcomeStat struct {
	name                   string
	Outcomes               map[string]*MultiOutcomeStat
	startsRecord           map[string]struct{}
	printOrder             []string
	negativeOutcomeOnNoEnd bool
	curStart               string
	started                bool
}

func (stat *MutliStartMultiOutcomeStat) Begin(start string) {
	//stat.closeOpenEnds()
	if _, exists := stat.startsRecord[start]; exists {
		//	stat.closeOpenEnds()
		stat.curStart = start
		stat.started = true
		stat.Outcomes[start].started++
	} else {
		panic("No record of this start: " + start)
	}
}

func (stat *MutliStartMultiOutcomeStat) End(outcome string) {
	if stat.started != true {
		panic("Not started multi-stage stat")
	}
	stat.Outcomes[stat.curStart].outcomes[outcome]++
	stat.started = false
}

func (stat *MutliStartMultiOutcomeStat) closeOpenEnds() {
	//	if stat.started { // if we start a new stat and didn't finish previous one, assume it is negative
	//		startedStat := stat.Outcomes[stat.curStart]
	//		startedStat.outcomes[startedStat.negative]++
	//	}
	for _, ongoing := range stat.Outcomes {
		ongoing.correctNegativeOutcomes()
	}
}

func (stat *MutliStartMultiOutcomeStat) OutputResult() string {
	stat.closeOpenEnds()

	str := strings.Builder{}
	for i, statStart := range stat.printOrder {
		j := 0
		for _, statSEVal := range stat.Outcomes[statStart].outcomes {
			if i == len(stat.printOrder)-1 && j == len(stat.Outcomes[statStart].outcomes) {
				str.WriteString(fmt.Sprintf("%d", statSEVal))
			} else {
				str.WriteString(fmt.Sprintf("%d, ", statSEVal))
			}
			j++
		}
	}
	return str.String()
}

func (stat *MutliStartMultiOutcomeStat) getOutputField() string {
	str := strings.Builder{}
	n := 0
	end := len(stat.printOrder) - 1
	for _, statStart := range stat.printOrder {
		end += len(stat.Outcomes[statStart].outcomes) - 1
		for statEndName, _ := range stat.Outcomes[statStart].outcomes {
			if n == end {
				str.WriteString(fmt.Sprintf("%s %s %s, ", stat.name, statStart, statEndName))
			} else {
				str.WriteString(fmt.Sprintf("%s %s %s, ", stat.name, statStart, statEndName))
			}
			n++
		}
	}
	log.Println(str.String())
	return str.String()
}

type MultiOutcomeStat struct {
	started    int
	outcomes   map[string]int
	negative   string
	printOrder []string
}

func (stat *MultiOutcomeStat) start() {
	stat.started++
}

func (stat *MultiOutcomeStat) correctNegativeOutcomes() {
	sumOutcomes := 0
	for _, v := range stat.outcomes {
		sumOutcomes = v
	}
	if sumOutcomes < stat.started {
		stat.outcomes[stat.negative] = sumOutcomes - stat.started
	}
}

func (stat *MultiOutcomeStat) RecordOutcome(outcome string) {
	if _, exists := stat.outcomes[outcome]; exists {

	} else {
		panic("No record of this outcome: " + outcome)
	}
}

type InstanceStats struct {
	outputFile *os.File
	*commitExecutionComparator
	register                map[InstanceID]map[string]int
	orderedKeys             []string
	complexStatIDs          map[string]int
	complexStats            map[InstanceID][]*MutliStartMultiOutcomeStat
	complexStatConstructors []MultiStartMultiOutStatConstructor
}

func InstanceStatsNew(outputLoc string, registerIDs []string, complexStatsConstructors []MultiStartMultiOutStatConstructor) *InstanceStats {
	file, _ := os.Create(outputLoc)

	registerIDs = append(registerIDs, "Ballot Choosing Attempts")
	statNames := make(map[string]int)
	for i, complexStat := range complexStatsConstructors {
		statNames[complexStat.Name] = i
	}
	instanceStats := &InstanceStats{
		outputFile:                file,
		commitExecutionComparator: commitExecutionComparatorNew(),
		orderedKeys:               registerIDs,
		register:                  make(map[InstanceID]map[string]int),
		complexStatIDs:            statNames,
		complexStatConstructors:   complexStatsConstructors,
		complexStats:              make(map[InstanceID][]*MutliStartMultiOutcomeStat),
	}

	str := strings.Builder{}
	str.WriteString("Log ID, Log Seq No")
	for i := 0; i < len(registerIDs); i++ {
		str.WriteString(fmt.Sprintf(", %s", registerIDs[i]))
	}
	str.WriteString(", ")
	for _, cStat := range instanceStats.complexStatConstructors {
		for _, start := range cStat.Starts {
			for _, end := range cStat.Outcomes[start] {
				str.WriteString(fmt.Sprintf("%s %s %s, ", cStat.Name, start, end))
			}
		}
	}
	str.WriteString(instanceStats.commitExecutionComparator.getOutputFields())
	str.WriteString("\n")
	file.WriteString(str.String())

	return instanceStats
}

func (stats *InstanceStats) checkAndInitialiseInstance(inst InstanceID) {
	if _, exists := stats.register[inst]; !exists {
		instanceRegister := make(map[string]int)
		for i := 0; i < len(stats.orderedKeys); i++ {
			instanceRegister[stats.orderedKeys[i]] = 0
		}
		stats.register[inst] = instanceRegister
		stats.complexStats[inst] = make([]*MutliStartMultiOutcomeStat, len(stats.complexStatConstructors))
		for _, statConstructor := range stats.complexStatConstructors {
			stats.complexStats[inst][stats.complexStatIDs[statConstructor.Name]] = statConstructor.Construct()
		}
	}
}

func (stat *InstanceStats) RecordComplexStatStart(inst InstanceID, statName, start string) {
	if v, e := stat.complexStatIDs[statName]; e {
		stat.complexStats[inst][v].Begin(start)
	}
}

func (stat *InstanceStats) RecordComplexStatEnd(inst InstanceID, statName, end string) {
	if v, e := stat.complexStatIDs[statName]; e {
		stat.complexStats[inst][v].End(end)
	}
}

func (stats *InstanceStats) RecordOccurrence(inst InstanceID, stat string, count int) {
	stats.checkAndInitialiseInstance(inst)
	stats.register[inst][stat] = stats.register[inst][stat] + count
}

func (stats *InstanceStats) RecordOpened(id InstanceID, time time.Time) {
	stats.checkAndInitialiseInstance(id)
	stats.commitExecutionComparator.recordOpen(id, time)
}

func (stats *InstanceStats) RecordCommitted(id InstanceID, atmts int, time time.Time) {
	stats.checkAndInitialiseInstance(id)
	stats.register[id]["Ballot Choosing Attempts"] = atmts
	stats.commitExecutionComparator.recordCommit(id, time)
}

func (stats *InstanceStats) RecordExecuted(id InstanceID, time time.Time) {
	if _, exists := stats.register[id]; !exists {
		panic("Oh no, we have executed without committing apparently! Maybe we just forgot to record it??")
	}
	stats.commitExecutionComparator.recordExecution(id, time)
}

func (stats *InstanceStats) OutputRecord(id InstanceID) {
	// needs instance and all prev instances to be executed and printed --
	//can rely on the fact that inst i can only call exec if all j < i are also executed
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("%d, %d, ", id.Log, id.Seq))
	for i := 0; i < len(stats.orderedKeys); i++ {
		k := stats.orderedKeys[i]
		v := stats.register[id][k]
		str.WriteString(fmt.Sprintf("%d, ", v))
	}
	for _, cStat := range stats.complexStats[id] {
		str.WriteString(cStat.OutputResult() + ", ")
	}
	cmtExecCmpStr := stats.commitExecutionComparator.outputInstanceTimes(id)
	str.WriteString(cmtExecCmpStr)
	str.WriteString("\n")
	stats.outputFile.WriteString(str.String())
	delete(stats.register, id)
	delete(stats.commitExecutionComparator.cmdsTimes, id)
	delete(stats.complexStats, id)
}

func (stats *InstanceStats) Close() {
	stats.outputFile.Close()
}
