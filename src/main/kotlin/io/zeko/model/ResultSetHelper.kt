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

package io.zeko.model

import com.github.jasync.sql.db.ResultSet
import io.vertx.core.json.JsonArray
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowSet
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

        fun toMaps(rows: ResultSet, columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
            val results = ArrayList<LinkedHashMap<String, Any?>>(rows.size)
            for (row in rows) {
                results.add(convertRowToMap(row.toList(), columns, timeProcessor, tzFrom, tzTo))
            }
            return results
        }

        fun toMaps(rs: java.sql.ResultSet, columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
            val results = java.util.ArrayList<LinkedHashMap<String, Any?>>()
            val md = rs.metaData
            val totalColumns = md.columnCount

            while (rs.next()) {
                val row = arrayListOf<Any?>()
                for (i in 1..totalColumns) {
                    row.add(rs.getObject(i))
                }
                results.add(convertRowToMap(row, columns, timeProcessor, tzFrom, tzTo))
            }
            return results
        }

        fun toMaps(rowSet: RowSet<Row>, columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<java.util.LinkedHashMap<String, Any?>> {
            val results = ArrayList<java.util.LinkedHashMap<String, Any?>>(rowSet.size())
            for (row in rowSet) {
                val size: Int = row.size()
                val rowList = arrayListOf<Any?>()

                for (pos in 0 until size) {
                    rowList.add(row.getValue(pos))
                }
                results.add(convertRowToMap(rowList, columns, timeProcessor, tzFrom, tzTo))
            }
            return results
        }

        fun convertRowToMap(row: List<Any?>, columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): LinkedHashMap<String, Any?> {
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

        fun <T> rowSetToDataObject(result: RowSet<Row>, dataClassHandler: (dataMap: Map<String, Any?>) -> T): List<T> {
            return result.map { jObj ->
                val rowMap = hashMapOf<String, Any?>()
                val size: Int = jObj.size()

                for (pos in 0 until size) {
                    val name: String = jObj.getColumnName(pos).toLowerCase()
                    val value: Any? = jObj.getValue(pos)
                    rowMap[name] = value
                }
                dataClassHandler(rowMap)
            }
        }
    }
}
