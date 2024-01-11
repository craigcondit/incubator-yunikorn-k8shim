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

package historyserver

type Mapper struct {
	appIDs        *[2]*Dictionary
	taskIDs       *[2]*Dictionary
	taskNames     *[2]*Dictionary
	userNames     *[2]*Dictionary
	nodeNames     *[2]*Dictionary
	instanceTypes *[2]*Dictionary
	resourceIDs   *[2]*Dictionary
}

func NewMapper() *Mapper {
	return &Mapper{}
}

func (m *Mapper) ApplicationIDs(old, new *Dictionary) {
	m.appIDs = &[2]*Dictionary{old, new}
}

func (m *Mapper) TaskIDs(old, new *Dictionary) {
	m.taskIDs = &[2]*Dictionary{old, new}
}

func (m *Mapper) TaskNames(old, new *Dictionary) {
	m.taskNames = &[2]*Dictionary{old, new}
}

func (m *Mapper) UserNames(old, new *Dictionary) {
	m.userNames = &[2]*Dictionary{old, new}
}

func (m *Mapper) NodeNames(old, new *Dictionary) {
	m.nodeNames = &[2]*Dictionary{old, new}
}

func (m *Mapper) InstanceTypes(old, new *Dictionary) {
	m.instanceTypes = &[2]*Dictionary{old, new}
}

func (m *Mapper) ResourceIDs(old, new *Dictionary) {
	m.resourceIDs = &[2]*Dictionary{old, new}
}

func (m *Mapper) MapApplicationID(value uint32) (uint32, bool) {
	return m.mapValue(value, m.appIDs)
}

func (m *Mapper) MapTaskID(value uint32) (uint32, bool) {
	return m.mapValue(value, m.taskIDs)
}

func (m *Mapper) MapTaskName(value uint32) (uint32, bool) {
	return m.mapValue(value, m.taskNames)
}

func (m *Mapper) MapUserName(value uint32) (uint32, bool) {
	return m.mapValue(value, m.userNames)
}

func (m *Mapper) MapNodeName(value uint32) (uint32, bool) {
	return m.mapValue(value, m.nodeNames)
}

func (m *Mapper) MapInstanceType(value uint32) (uint32, bool) {
	return m.mapValue(value, m.instanceTypes)
}

func (m *Mapper) MapResourceID(value uint32) (uint32, bool) {
	return m.mapValue(value, m.resourceIDs)
}

func (m *Mapper) mapValue(value uint32, dict *[2]*Dictionary) (uint32, bool) {
	if dict == nil || dict[0] == nil || dict[1] == nil {
		return 0, false
	}
	symbol, ok := dict[0].LookupSymbol(value)
	if !ok {
		return 0, false
	}
	return dict[1].LookupID(symbol)
}
