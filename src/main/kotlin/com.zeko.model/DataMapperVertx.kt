/**
 *  Copyright (c) 2017 Leng Sheng Hong
 *  ------------------------------------------------------
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zeko.model

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

class DataMapperVertx: DataMapper() {

    fun mapJsonObjects(allTableInfo: LinkedHashMap<String, TableInfo>, arr: List<JsonObject>, delimiter: String): ArrayList<LinkedHashMap<String, Any>>? { //ArrayList<LinkedHashMap<String, Any>>? {
        if (arr.size == 0) {
            return arrayListOf()
        }
        return super.map(allTableInfo, arr, delimiter)
    }

    fun mapJsonArray(allTableInfo: LinkedHashMap<String, TableInfo>, arr: JsonArray, delimiter: String): ArrayList<LinkedHashMap<String, Any>>? { //ArrayList<LinkedHashMap<String, Any>>? {
        if (arr.size() == 0) {
            return arrayListOf()
        }
        return super.map(allTableInfo, (arr.list as List<JsonObject>), delimiter)
    }

    override fun convertToMap(oriData: Any?): LinkedHashMap<String, Any?>? {
        var dataNew = LinkedHashMap<String, Any?>()

        if (oriData is JsonObject) {
            dataNew = oriData.map as LinkedHashMap<String, Any?>
        } else {
            dataNew = oriData as LinkedHashMap<String, Any?>
        }
        return dataNew
    }
}