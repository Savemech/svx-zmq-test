package main

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	zmq "github.com/pebbe/zmq4"
)

func getISOTime() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
}

func main() {
	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.REQ)
	defer socket.Close()
	socket.Connect("tcp://localhost:5555")

	fmt.Printf("%s - Client started\n", getISOTime())

	// Create 150,000 key-value records
	createStart := time.Now()
	createTimes := make([]time.Duration, 150000)

	for i := 0; i < 150000; i++ {
		key := fmt.Sprintf("%d", i)
		value := fmt.Sprintf("value-%d", rand.Intn(1000000))

		startTime := time.Now()
		socket.Send(fmt.Sprintf("SET %s %s", key, value), 0)
		_, err := socket.Recv(0)
		if err != nil {
			fmt.Printf("Error setting key %s: %v\n", key, err)
		}
		duration := time.Since(startTime)
		createTimes[i] = duration

		if i%10000 == 0 {
			fmt.Printf("%s - Created %d records\n", getISOTime(), i)
		}
	}

	createDuration := time.Since(createStart)
	fmt.Printf("%s - Created 150,000 records in %v\n", getISOTime(), createDuration)

	// Perform 50,000 random reads
	readStart := time.Now()
	readTimes := make([]time.Duration, 50000)
	sendTimes := make([]time.Duration, 50000)
	receiveTimes := make([]time.Duration, 50000)

	for i := 0; i < 50000; i++ {
		key := fmt.Sprintf("%d", rand.Intn(150000))

		sendStartTime := time.Now()
		socket.Send(fmt.Sprintf("GET %s", key), 0)
		sendTime := time.Since(sendStartTime)

		startReceiveTime := time.Now()
		_, err := socket.Recv(0)
		if err != nil {
			fmt.Printf("Error getting key %s: %v\n", key, err)
		}
		receiveTime := time.Since(startReceiveTime)

		totalTime := sendTime + receiveTime
		readTimes[i] = totalTime
		sendTimes[i] = sendTime
		receiveTimes[i] = receiveTime

		if i%5000 == 0 {
			fmt.Printf("%s - Read %d records\n", getISOTime(), i)
		}
	}

	readDuration := time.Since(readStart)
	fmt.Printf("%s - Read 50,000 records in %v\n", getISOTime(), readDuration)

	// Calculate and print statistics
	printStats("Create times", createTimes)
	printStats("Read times (total)", readTimes)
	printStats("Read times (send)", sendTimes)
	printStats("Read times (receive)", receiveTimes)
}

func printStats(name string, times []time.Duration) {
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })

	var sum time.Duration
	for _, t := range times {
		sum += t
	}

	avg := sum / time.Duration(len(times))
	min := times[0]
	max := times[len(times)-1]

	fmt.Printf("%s:\n", name)
	fmt.Printf("  Average: %v\n", avg)
	fmt.Printf("  Min: %v\n", min)
	fmt.Printf("  Max: %v\n", max)
}
