/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package main

import (
	"fmt"
	"time"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-k8shim/pkg/historyserver"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func main() {
	log.Log(log.Test).Info("START")
	test()
	log.Log(log.Test).Info("END")
}

func test() {
	// create some in-memory dictionaries
	appIDs := historyserver.NewDictionary()
	taskIDs := historyserver.NewDictionary()
	taskNames := historyserver.NewDictionary()
	userNames := historyserver.NewDictionary()
	nodeNames := historyserver.NewDictionary()
	instanceTypes := historyserver.NewDictionary()
	resourceIDs := historyserver.NewDictionary()

	// create some dummy data
	users := []string{"tom", "dick", "harry", "alice", "bob"}
	nodes := []string{"node0001.example.com", "node0002.example.com", "node0003.example.com", "node0004.example.com"}
	instances := []string{"normal", "normal", "normal", "gpu"}
	resources := []string{"vcore", "memory", "pods", "nvidia.com/gpu", "ephemeral-storage", "hugepages-1Gi", "hugepages-2Mi"}
	quantities := []uint64{1000, 1024 * 1024 * 1024, 1, 1, 10000000000, 1024 * 1024 * 1024, 2 * 1024 * 1024}

	// generate log entries
	wal, err := historyserver.NewWriteAheadLogWriter("/tmp/historyserver/raw-data.wal")
	if err != nil {
		panic(err)
	}

	// create an in-memory time series
	series := historyserver.NewInMemoryTimeSeries()

	records := 0
	log.Log(log.Test).Info("Generating and processing records")
	i := 0
	for i = 0; i < 1000; i++ {
		if i%100 == 0 {
			log.Log(log.Test).Info("Batch", zap.Int("i", i), zap.Int("records", records))
		}
		for j := 0; j < 100; j++ {
			applicationID := fmt.Sprintf("application_%04d", j)
			taskID := uuid.New().String()
			taskName := fmt.Sprintf("task_%04d_%06d", j, i)
			userName := users[j%5]
			nodeName := nodes[i%4]
			instanceType := instances[i%4]

			startTime := time.Now().Add(-60 * time.Minute)

			for k := 0; k < 12; k++ {
				for idx, resourceID := range resources {
					if j%len(resources) > 3 {
						continue
					}
					records++
					ts := startTime.Add(time.Duration(5*k) * time.Minute)
					startTimeSec := uint32(ts.Sub(startTime).Milliseconds() / 1000)

					walEntry := &historyserver.WriteAheadLogEntry{
						ApplicationID: applicationID,
						TaskID:        taskID,
						TaskName:      taskName,
						UserName:      userName,
						NodeName:      nodeName,
						InstanceType:  instanceType,
						ResourceID:    resourceID,
						EventTime:     ts,
						DurationSecs:  300,
						Quantity:      quantities[idx],
					}
					if err := wal.WriteEntry(walEntry); err != nil {
						panic(err)
					}

					// register the symbols as we come across them
					appIDKey := appIDs.AddSymbol(applicationID)
					taskNameKey := taskNames.AddSymbol(taskName)
					userNameKey := userNames.AddSymbol(userName)
					nodeNameKey := nodeNames.AddSymbol(nodeName)
					instanceTypeKey := instanceTypes.AddSymbol(instanceType)
					taskIDKey := taskIDs.AddSymbol(taskID)
					resourceIDKey := resourceIDs.AddSymbol(resourceID)

					// add entry to time series
					series.PutData(taskIDKey, resourceIDKey,
						historyserver.TSMetadata{
							ApplicationID: appIDKey,
							UserName:      userNameKey,
							TaskName:      taskNameKey,
							NodeName:      nodeNameKey,
							InstanceType:  instanceTypeKey,
						},
						historyserver.TSData{
							StartTimeSecs: startTimeSec,
							DurationSecs:  300,
							Quantity:      quantities[idx],
						})
				}

			}

		}
	}
	log.Log(log.Test).Info("Processed all records", zap.Int("batches", i), zap.Int("records", records))

	if err := wal.Close(); err != nil {
		panic(err)
	}
	log.Log(log.Test).Info("WAL closed", zap.Uint64("size", wal.GetSize()))

	mapper := historyserver.NewMapper()

	log.Log(log.Test).Info("Sorting and writing dictionaries...")
	sortedAppIDs := appIDs.Sort()
	if err := sortedAppIDs.Save("/tmp/historyserver/application-id.dict"); err != nil {
		panic(err)
	}
	mapper.ApplicationIDs(appIDs, sortedAppIDs)

	sortedTaskIDs := taskIDs.Sort()
	if err := sortedTaskIDs.Save("/tmp/historyserver/task-id.dict"); err != nil {
		panic(err)
	}
	mapper.TaskIDs(taskIDs, sortedTaskIDs)

	sortedTaskNames := taskNames.Sort()
	if err := sortedTaskNames.Save("/tmp/historyserver/task-name.dict"); err != nil {
		panic(err)
	}
	mapper.TaskNames(taskNames, sortedTaskNames)

	sortedUserNames := userNames.Sort()
	if err := sortedUserNames.Save("/tmp/historyserver/user-name.dict"); err != nil {
		panic(err)
	}
	mapper.UserNames(userNames, sortedUserNames)

	sortedNodeNames := nodeNames.Sort()
	if err := sortedNodeNames.Save("/tmp/historyserver/node-name.dict"); err != nil {
		panic(err)
	}
	mapper.NodeNames(nodeNames, sortedNodeNames)

	sortedInstanceTypes := instanceTypes.Sort()
	if err := sortedInstanceTypes.Save("/tmp/historyserver/instance-type.dict"); err != nil {
		panic(err)
	}
	mapper.InstanceTypes(instanceTypes, sortedInstanceTypes)

	sortedResourceIDs := resourceIDs.Sort()
	if err := sortedResourceIDs.Save("/tmp/historyserver/resource-id.dict"); err != nil {
		panic(err)
	}
	mapper.ResourceIDs(resourceIDs, sortedResourceIDs)

	log.Log(log.Test).Info("Done writing dictionaries.")

	// re-key the time series data
	log.Log(log.Test).Info("Rekeying time series...")
	if ok := series.ReKey(mapper); !ok {
		log.Log(log.Test).Warn("Rekeying time series was incomplete (found missing keys)")
	}
	log.Log(log.Test).Info("Rekeying time series complete.")

	// save the time series data (and primary key
	log.Log(log.Test).Info("Writing time series to disk...")
	series.Save("/tmp/historyserver/metrics-pk.index", "/tmp/historyserver/metrics.data")

	// read the WAL back in
	log.Log(log.Test).Info("Reading WAL")
	walReader, err := historyserver.NewWriteAheadLogReader("/tmp/historyserver/raw-data.wal")
	if err != nil {
		panic(err)
	}
	count := 0
	for {
		entry, ok, err := walReader.ReadEntry()
		if err != nil {
			log.Log(log.Test).Warn("Failed to read WAL entry", zap.Error(err))
			break
		}
		if !ok {
			log.Log(log.Test).Info("WAL EOF encountered")
			break
		}
		count++
		if count%100000 == 0 {
			log.Log(log.Test).Info("Read entries", zap.Int("count", count))
			log.Log(log.Test).Info("Sample entry",
				zap.String("applicationID", entry.ApplicationID),
				zap.String("taskID", entry.TaskID),
				zap.String("taskName", entry.TaskName),
				zap.String("userName", entry.UserName),
				zap.String("nodeName", entry.NodeName),
				zap.String("instanceType", entry.InstanceType),
				zap.String("resourceID", entry.ResourceID),
				zap.Time("EventTime", entry.EventTime),
				zap.Uint32("DurationSecs", entry.DurationSecs),
				zap.Uint64("quantity", entry.Quantity))
		}
	}

	if err = walReader.Close(); err != nil {
		panic(err)
	}

	log.Log(log.Test).Info("Finished reading WAL",
		zap.Int("count", count), zap.Uint64("size", walReader.GetSize()))

	// once the sorted dictionaries are created, we will need to update the data structures we have created with
	// the new IDs; this should just involve iterating each entry and looking up the existing key in the old dictionary
	// and then mapping it to the new ID in the sorted dictionary.

	// creating an in-memory DB that is both appendable and queryable efficiently may be a bit challenging. The indexes
	// will not be sorted until we finalize what we have to disk. While reconciling, the entire DB needs to be
	// frozen, as it will not be consistent until the process completes.

	// we can create multiple shards per partition, and write a new shard once our data size has grown too large.
	// additionally if we have no open shards in a partition, we can create a new one and keep it open for a period of
	// time (say 5 minutes or so).

	// if the number of shards grows too large, we can merge them with a background process and then atomically replace
	// the shards with the merged data. this should help ensure that we don't consume too much RAM.

	// once a shard is "finalized", it becomes read-only and we can use mmap'd i/o to read from it. we need store only
	// the dictionaries in memory (which will be very small).
}
