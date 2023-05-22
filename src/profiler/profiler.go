package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/net"
)

var outputFile = flag.String("o", "", "Must state where resultant latencies will be written")
var sampleRateMs = flag.Int("samplerate", 1000, "how often to sample timeseries data (ms)")
var nicNum = flag.Int("nic", 0, "which nic ??????????")
var nicnName = flag.String("nicname", "", "which nic name ??????????")
var diskName = flag.String("disk", "sda0", "which nic disk")

func getTimestamp() string {
	return fmt.Sprintf("%s, %d,", time.Now().Format("2006/01/02 15:04:05 .000"), time.Now().UnixNano())
}

type profilerDiff struct {
	diskPrev disk.IOCountersStat
	netPrev  net.IOCountersStat
	curNet   net.IOCountersStat
	curDisk  disk.IOCountersStat

	nic  int
	disk string
}

func (p *profilerDiff) calcDiskDiff() disk.IOCountersStat {
	diff := disk.IOCountersStat{}
	diff.ReadCount = p.curDisk.ReadCount - p.diskPrev.ReadCount
	diff.WriteCount = p.curDisk.WriteCount - p.diskPrev.WriteCount
	diff.ReadBytes = p.curDisk.ReadBytes - p.diskPrev.ReadBytes
	diff.WriteBytes = p.curDisk.WriteBytes - p.diskPrev.WriteBytes
	return diff
}

func (p *profilerDiff) calcNetDiff() net.IOCountersStat {
	diff := net.IOCountersStat{}
	diff.PacketsSent = p.curNet.PacketsSent - p.netPrev.PacketsSent
	diff.PacketsRecv = p.curNet.PacketsRecv - p.netPrev.PacketsRecv
	diff.BytesSent = p.curNet.BytesSent - p.netPrev.BytesSent
	diff.BytesRecv = p.curNet.BytesRecv - p.netPrev.BytesRecv
	diff.Dropout = p.curNet.Dropout - p.netPrev.Dropout
	diff.Dropin = p.curNet.Dropin - p.netPrev.Dropin
	return diff
}

func (p *profilerDiff) updateNext() {
	nics, _ := net.IOCounters(true)
	disks, _ := disk.IOCounters()
	p.curNet = nics[p.nic]
	p.curDisk = disks[p.disk]
}

func (p *profilerDiff) updateCurDisk() {
	disks, _ := disk.IOCounters()
	p.curDisk = disks[p.disk]
}

func (p *profilerDiff) updateCurNet() {
	nics, _ := net.IOCounters(true)
	p.curNet = nics[p.nic]
}

func (p *profilerDiff) setDiskPrev() {
	p.diskPrev = p.curDisk
}

func (p *profilerDiff) setNetPrev() {
	p.netPrev = p.curNet
}

func profileCPURNetworkAndDisk(diff *profilerDiff) string {
	cpuPercent, _ := cpu.Percent(time.Second, false)
	diff.updateNext()

	disk := diff.calcDiskDiff()
	nic := diff.calcNetDiff()
	s := strings.Builder{}
	s.WriteString(getTimestamp())
	s.WriteString(fmt.Sprintf(" %.2f,", cpuPercent[0]))
	s.WriteString(fmt.Sprintf(" %d, %d, %d, %d, %d, %d,", nic.PacketsSent, nic.PacketsRecv, nic.BytesSent, nic.BytesRecv, nic.Dropout, nic.Dropin))
	s.WriteString(fmt.Sprintf(" %d, %d, %d, %d", disk.ReadCount, disk.WriteCount, disk.ReadBytes, disk.WriteBytes))
	s.WriteString("\n")
	return s.String()
}

//// Function to profile and write disk load to a file with timestamps
//func profileDisk(filePath string) {
//	for {
//		counters, _ := disk.IOCounters()
//		timestamp := time.Now().Format("2006/01/02 15:04:05.000")
//		data := fmt.Sprintf("%s, %d, %d\n", timestamp, counters[0].ReadBytes, counters[0].WriteBytes)
//		writeToFile(filePath, data)
//	}
//}

// Function to write data to a file
func writeToFile(f *os.File, data string) {
	if _, err := f.WriteString(data); err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}

func diffNew(nic int, nicName string, diskName string) profilerDiff {
	nics, _ := net.IOCounters(true)
	if nics[nic].Name != nicName {
		panic(fmt.Sprintf("Expected nic name %s does not match actual nic name %s", nicName, nics[nic].Name))
	}
	disks, _ := disk.IOCounters()
	curNet := nics[nic]
	curDisk := disks[diskName]
	diff := profilerDiff{
		diskPrev: curDisk,
		netPrev:  curNet,
		curNet:   net.IOCountersStat{},
		curDisk:  disk.IOCountersStat{},
		nic:      nic,
		disk:     diskName,
	}
	return diff
}

func main() {
	flag.Parse()

	shutdown := false
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill)

	f, err := os.Create(*outputFile)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	// write csv header
	writeToFile(f, "Human Time, Robot Time, CPU Usage, Packets Sent, Packets Received, Bytes Sent, Bytes Received, Dropped Packets In, Dropped Packets Out, Disk Read Count, Disk Write Count, Disk Read Bytes, Disk Write Bytes\n")

	// initialise diff
	diff := diffNew(*nicNum, *nicnName, *diskName)
	statsTimer := time.NewTimer(time.Duration(*sampleRateMs) * time.Millisecond)
	for !shutdown {
		select {
		case <-interrupt:
			// close program
			f.Sync()
			f.Close()
			shutdown = true
			break
		case <-statsTimer.C:
			// write output
			profile := profileCPURNetworkAndDisk(&diff)
			diff.setNetPrev()
			diff.setDiskPrev()
			writeToFile(f, profile)
			statsTimer = time.NewTimer(time.Duration(*sampleRateMs) * time.Millisecond)
			break
		}
	}
}
