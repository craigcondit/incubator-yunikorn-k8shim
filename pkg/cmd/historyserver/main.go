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

const Metric = "m"
const ApplicationID = "a"
const TaskID = "t"
const NodeID = "n"
const InstanceTypeID = "i"
const DurationSec = "d"
const ResourceType = "r"

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
	resourceIDs.AddSymbol("vcore")
	resourceIDs.AddSymbol("memory")
	resourceIDs.AddSymbol("pods")
	resourceIDs.AddSymbol("nvidia.com/gpu")
	resourceIDs.AddSymbol("ephemeral-storage")
	resourceIDs.AddSymbol("hugepages-1Gi")
	resourceIDs.AddSymbol("hugepages-2Mi")

	// generate log entries
	wal, err := historyserver.NewWriteAheadLogWriter("/tmp/historyserver/wal.dat")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		log.Log(log.Test).Info("Batch", zap.Int("i", i))
		for j := 0; j < 100; j++ {
			applicationID := fmt.Sprintf("application_%04d", j)
			taskID := uuid.New().String()
			taskName := fmt.Sprintf("task_%04d_%06d", j, i)
			userName := users[j%5]
			nodeName := nodes[i%4]
			instanceType := instances[i%4]

			// register the symbols as we come across them
			appIDs.AddSymbol(applicationID)
			taskIDs.AddSymbol(taskID)
			taskNames.AddSymbol(taskName)
			userNames.AddSymbol(userName)
			nodeNames.AddSymbol(nodeName)
			instanceTypes.AddSymbol(instanceType)

			for i, rid := range resources {
				ts := time.Now()
				walEntry := &historyserver.WriteAheadLogEntry{
					ApplicationID: applicationID,
					TaskID:        taskID,
					TaskName:      taskName,
					UserName:      userName,
					NodeName:      nodeName,
					InstanceType:  instanceType,
					ResourceID:    rid,
					EventTime:     ts,
					DurationSecs:  300,
					Quantity:      quantities[i],
				}
				if err := wal.WriteEntry(walEntry); err != nil {
					panic(err)
				}
			}
		}
	}

	if err := wal.Close(); err != nil {
		panic(err)
	}

	sortedAppIDs := appIDs.Sort()
	if err := sortedAppIDs.Save("/tmp/historyserver/dict-appid.dat"); err != nil {
		panic(err)
	}
	sortedTaskIDs := taskIDs.Sort()
	if err := sortedTaskIDs.Save("/tmp/historyserver/dict-taskid.dat"); err != nil {
		panic(err)
	}
	sortedTaskNames := taskNames.Sort()
	if err := sortedTaskNames.Save("/tmp/historyserver/dict-taskname.dat"); err != nil {
		panic(err)
	}
	sortedUserNames := userNames.Sort()
	if err := sortedUserNames.Save("/tmp/historyserver/dict-username.dat"); err != nil {
		panic(err)
	}
	sortedNodeNames := nodeNames.Sort()
	if err := sortedNodeNames.Save("/tmp/historyserver/dict-nodename.dat"); err != nil {
		panic(err)
	}
	sortedInstanceTypes := instanceTypes.Sort()
	if err := sortedInstanceTypes.Save("/tmp/historyserver/dict-instancetype.dat"); err != nil {
		panic(err)
	}
	sortedResourceIDs := resourceIDs.Sort()
	if err := sortedResourceIDs.Save("/tmp/historyserver/dict-resourceid.dat"); err != nil {
		panic(err)
	}

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
