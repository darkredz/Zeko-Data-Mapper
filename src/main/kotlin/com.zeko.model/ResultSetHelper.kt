/**
 *  Copyright (c) 2018 Leng Sheng Hong
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

import com.github.jasync.sql.db.ResultSet
import io.vertx.core.json.JsonArray
import org.joda.time.DateTimeZone
import org.joda.time.LocalDateTime
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import org.joda.time.base.BaseLocal
import java.util.LinkedHashMap

class ResultSetHelper {
    companion object {

        fun toMaps(rows: List<JsonArray>, columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone?, tzTo: DateTimeZone?): List<LinkedHashMap<String, Any?>> {
            val results = ArrayList<LinkedHashMap<String, Any?>>(rows.size)
            for (row in rows) {
                results.add(convertRowToMap(row.toList(), columns, timeProcessor, tzFrom, tzTo))
            }
            return results
        }

        fun toMaps(rows: ResultSet, columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone?, tzTo: DateTimeZone?): List<LinkedHashMap<String, Any?>> {
            val results = ArrayList<LinkedHashMap<String, Any?>>(rows.size)
            for (row in rows) {
                results.add(convertRowToMap(row.toList(), columns, timeProcessor, tzFrom, tzTo))
            }
            return results
        }

        fun convertRowToMap(row: List<Any?>, columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone?, tzTo: DateTimeZone?): LinkedHashMap<String, Any?> {
            val obj = java.util.LinkedHashMap<String, Any?>()

            for ((i, value) in row.withIndex()) {
                val colName = columns[i]
                if (timeProcessor != null) {
                    obj[colName] = when (value) {
                        is LocalDateTime -> timeProcessor(value, tzFrom, tzTo)
                        is LocalDate -> timeProcessor(value, tzFrom, tzTo)
                        is LocalTime -> timeProcessor(value, tzFrom, tzTo)
                        else -> value
                    }
                } else {
                    obj[colName] = when (value) {
                        is LocalDateTime -> DateTimeHelper.toDateTimeStrUTC(value)
                        is LocalDate -> DateTimeHelper.toDateTimeStrUTC(value)
                        is LocalTime -> DateTimeHelper.toDateTimeStrUTC(value)
                        else -> value
                    }
                }
            }
            return obj
        }
    }
}
