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

package io.zeko.model

import java.util.stream.Collectors

data class TableInfo(var key: String, var move_under: String? = null, var foreign_key: String? = null, var rename: String? = null,
                     var many_to_one: Boolean = false, var one_to_many: Boolean = false, var one_to_one: Boolean = false, var many_to_many: Boolean = false,
                     var multiple_link: Boolean = false, var remove: List<String>? = null, var dataClassHandler: ((dataMap: Map<String, Any?>) -> Any)? = null)

open class DataMapper {
    companion object {
        fun create(): DataMapper {
            return DataMapper()
        }
    }

    fun mapRaw(allTableInfo: LinkedHashMap<String, TableInfo>, arr: List<Any>, delimiter: String = "-", nested: Boolean = true, objectListWithID: Boolean = false): LinkedHashMap<String, LinkedHashMap<String, Any?>?> {
        var flattenResult = LinkedHashMap<String, LinkedHashMap<String, Any?>?>();
        var tables = allTableInfo

        for ((tblAlias, tblInfo) in tables) {
            val tblPrimeKey = tblInfo?.key
            var tblRename = ""

            if (nested) {
                tblRename = tblAlias
            }
            else {
                if (tblInfo?.rename != null) {
                    tblRename = tblInfo.rename as String
                }
                else {
                    tblRename = tblAlias
                }
            }

            var tblRows = flattenResult[tblRename]
            var tblHaveValues = false
            if (tblRows == null) {
                tblRows = LinkedHashMap<String, Any?>()
            }

            //map all row fields value to the appropriate table object based on the prefix.
            for (row0 in arr) {
                var row = convertToMap(row0)
                if (tblInfo == null || row == null) continue

                val rowPrimeKeyName = tblAlias + delimiter + tblPrimeKey
                val tblAliasItem = tables[tblAlias]
                if (tblAliasItem == null) continue
                tblAliasItem.multiple_link = (tblInfo.many_to_many || tblInfo.one_to_many)
                tables[tblAlias] = tblAliasItem

                val primeKeyVal = row[rowPrimeKeyName]

                //if table is not many to many, dun care about the prime key, as rows of same id is reused to link between two tables
                if (tblAliasItem.multiple_link == false) {
                    //if this table row already exists => check with primary key. then skip this row check for this table type.
                    if (primeKeyVal == null || tblRows[primeKeyVal] != null) continue
                }

                //map all related fields to the table attribute
                var obj: LinkedHashMap<String, Any?>? = null
                var allValues = 0
                var noValues = 0

                for ((field, value) in row) {
                    val fieldParts = field.split(delimiter)
                    val prefix = fieldParts[0]
                    if (prefix != tblAlias) continue
                    allValues++

                    val attr = fieldParts[1]
                    if (value == null) noValues++

                    //exclude field in remove list
                    val removeList = tblAliasItem.remove
                    if (!nested && removeList != null && removeList.indexOf(attr) > -1) continue

                    if (obj == null) {
                        obj = LinkedHashMap<String, Any?>()
                    }
                    obj.put(attr, value)
                }

                if (obj != null && noValues != allValues) {
                    if (!tblAliasItem.multiple_link) {
                        tblRows.put(primeKeyVal.toString(), obj)
                    }
                    else {
                        tblRows.put(tblRows.size.toString(), obj)
                    }
                    tblHaveValues = true
                }
            }

            if (tblRows != null) {
                if (tblHaveValues) {
                    flattenResult.put(tblRename, tblRows)
                } else {
                    flattenResult.put(tblRename, null)
                }
            }
        }


        if (!nested) {
            return flattenResult
        }

        if (nested) {
            val tablesRenamed = LinkedHashMap<String, TableInfo>();
            for ((key, va) in tables) {
                if (va?.rename != null) {
                    //also need to rename the move_under field if got rename set
                    if (tables[va.move_under]?.rename != null) {
                        val va2 = va.copy()
                        va2.move_under = tables[va.move_under]?.rename
                        tables[key] = va2
                    }
                    tablesRenamed[va.rename as String] = va
                }
            }

            if (tablesRenamed.size > 0) {
                //rename result table key to the renamed version
                for ((key, va) in flattenResult) {
                    if (tables.containsKey(key) && tables[key]?.rename != null) {
                        val tname: String = tables[key]?.rename!!
                        flattenResult[tname] = va
                        flattenResult.remove(key)
                    }
                }

                tables = tablesRenamed
            }

            //make a list of nested objects, move objects under each other appropriately based on their relationships
            for ((tbl, tblInfo) in tables) {
                var moveToTbl: LinkedHashMap<String, Any?>? = LinkedHashMap<String, Any?>()

                if (tblInfo?.move_under != null) {
                    if (flattenResult[tblInfo.move_under.toString()] == null) continue;
                    moveToTbl = convertToMap(flattenResult[tblInfo.move_under.toString()]!!)
                }

                val moveToTbl2 = moveToTbl?.clone() as LinkedHashMap<String, Any>

                for ((key, rows0) in moveToTbl2) {
                    // just move the original table result list/single, and park under the linked table object, no logic. remove them that are not there later
                    val oriTblResult = flattenResult[tbl]
                    val primeKeyName = tables[tbl]?.key
                    val rows = convertToMap(rows0)
                    if (rows == null) continue

                    val foreignKey = rows[primeKeyName]

                    if (tblInfo?.foreign_key != null) {
                        val fk = tblInfo.foreign_key
                        //loop self table and check if can link to the foreign table based on key => foreign key

                        //for many (many to many, one to many), remove those with same primary key (grouped since rows from query has a lot of same duplicates)
                        //many results are all stored, without group
                        if (tblInfo.many_to_many || tblInfo.one_to_many) {
                            //remove those that are not suppose to linked with the foreign table result attr.
                            val manyResults = LinkedHashMap<String, LinkedHashMap<String, Any?>?>()
                            if (oriTblResult != null) {
                                for ((linkKey, linkRow0) in oriTblResult) {

                                    var linkRow = convertToMap(linkRow0)

                                    if (tblInfo.dataClassHandler != null) {
                                        linkRow!!.put("_tbl", tbl)
                                    }

                                    if (linkRow == null) continue

                                    if (foreignKey != linkRow[fk]) {
                                        continue;
                                    }

                                    val linkRowPrimeKey = linkRow[primeKeyName]?.toString()
                                    if (manyResults[linkRowPrimeKey] != null) {
                                        continue
                                    }

                                    //exclude field in remove list
                                    if (tables[tbl]?.remove != null) {
                                        for (fieldToRemove in tables[tbl]?.remove!!) {
                                            linkRow.remove(fieldToRemove)
                                        }
                                    }

                                    if (linkRowPrimeKey != null) {
                                        manyResults.put(linkRowPrimeKey, linkRow)
                                    }
                                }
                                rows.put(tbl, manyResults)
                            }
                            else {
                                rows.put(tbl, null)
                            }
                        }
                        else if (tblInfo.many_to_one || tblInfo.one_to_one) {
                            if (oriTblResult == null) continue

                            val tblRowConvert = convertToMap(oriTblResult)
                            val tblRowConvert2 = tblRowConvert?.clone() as LinkedHashMap<String, Any>
                            rows.put(tbl, tblRowConvert2)

                            //remove those that are not suppose to linked with the foreign table result attr.
                            for ((linkRowKey, linkRow0) in tblRowConvert) {
                                val linkRow = convertToMap(linkRow0)
                                if (linkRow == null) continue

                                if (tblInfo.dataClassHandler != null) {
                                    linkRow.put("_tbl", tbl)
                                }

                                if (foreignKey != linkRow[fk]) {
                                    (rows[tbl] as LinkedHashMap<String, Any>).remove(linkRowKey)
                                    continue
                                }

                                //exclude field in remove list
                                if (tables[tbl]?.remove != null) {
                                    for (fieldToRemove in tables[tbl]?.remove!!) {
                                        val linkRowClone = (linkRow.clone() as LinkedHashMap<String, Any>)
                                        linkRowClone.remove(fieldToRemove)
                                        (rows[tbl] as LinkedHashMap<String, Any>)[linkRowKey] = linkRowClone
                                    }
                                }
                            }
                        }

                        //if the array is empty then just set as null
                        if (!rows.containsKey(tbl) || rows[tbl] == null) {
//                            rows[tbl] = null
                        }
                        else {
                            //if only one is set, the attribute should be just the object itself instead of an array of one object.
                            if (tblInfo.one_to_many || tblInfo.one_to_one) {
                                var firstItemKey = LinkedHashMap<String, Any>()
                                for ((itmKey, item) in (rows[tbl] as LinkedHashMap<String, Any>)) {
                                    firstItemKey = (item as LinkedHashMap<String, Any>)

                                    if (tblInfo.dataClassHandler != null) {
                                        item.put("_tbl", tbl)
                                    }
                                    break
                                }
                                rows[tbl] = firstItemKey
                            }
                        }
                    }
                }

                if (tblInfo?.move_under != null) {
                    flattenResult[tblInfo.move_under.toString()] = moveToTbl
                }

                //exclude field in remove list, this is for those that are not moved under, since moved under are already duplicated nested in.
                if (tblInfo!!.remove != null) {
                    val tblToChk = flattenResult[tbl]
                    if (tblToChk != null) {
                        for ((key, rows) in tblToChk) {
                            val rowClone = (rows as LinkedHashMap<String, Any>).clone() as LinkedHashMap<String, Any>

                            for ((attr, va) in (rows as LinkedHashMap<String, Any?>)) {
                                if (tblInfo!!.remove!!.contains(attr)) {
                                    rowClone.remove(attr)
                                    tblToChk[key] = rowClone
                                }
                            }
                        }
                    }
                }
            }
        }

        return flattenResult
    }

    fun flatMap(item: LinkedHashMap<String,*>): List<Any?> {
        val result = item.entries.stream()
                .map({ x ->
                    (x.value as LinkedHashMap<String,*>).remove("_tbl")
                    if (x.value is LinkedHashMap<*,*>) {
                        checkAndFlatMap(x.value as LinkedHashMap<String, Any>)
                    } else {
                        x.value
                    }
                })
                .collect(Collectors.toList<Any?>())

        return result
    }

    open fun checkAndFlatMap(toAdd: LinkedHashMap<String, *>): Any {
        val toAdd2: LinkedHashMap<String, Any?> = toAdd.clone() as LinkedHashMap<String, Any?>
        for ((key, itemNest) in toAdd) {
            if (itemNest is LinkedHashMap<*,*>) {
                if (itemNest.keys.isEmpty()) {
                    toAdd2[key] = null
                }
                else if (itemNest.keys.first().toString().matches(Regex("^\\d+$"))) {
                    toAdd2[key] = flatMap(itemNest as LinkedHashMap<String,*>)
                } else {
                    toAdd2[key] = checkAndFlatMap(itemNest as LinkedHashMap<String,*>)
                }
            }
        }
        return toAdd2
    }

    open fun map(mapConf: MapperConfig, arr: List<Any>, delimiter: String = "-"): ArrayList<LinkedHashMap<String, Any>> {
        return map(mapConf.toTableInfo(), arr)
    }

    open fun map(allTableInfo: LinkedHashMap<String, TableInfo>, arr: List<Any>, delimiter: String = "-"): ArrayList<LinkedHashMap<String, Any>> {
        if (arr.size == 0) {
            return arrayListOf()
        }
        val rs = mapRaw(allTableInfo, arr, delimiter, true, false)
        //get the root table result since everything now is nested into objects.
        if (rs != null && rs.size > 0) {
            val rootTable = rs.values.first() as LinkedHashMap<String, LinkedHashMap<String, Any>>
            val arrFinal = ArrayList<LinkedHashMap<String, Any>>()
            for ((key, item) in rootTable) {
                var toAdd: Any = item
                toAdd = checkAndFlatMap(toAdd as LinkedHashMap<String, Any>)
                arrFinal.add(toAdd as LinkedHashMap<String, Any>)
            }
            return arrFinal
        }
        return arrayListOf()
    }

    fun flatMapStruct(item: LinkedHashMap<String,*>, allTableInfo: LinkedHashMap<String, TableInfo>): List<Any?> {
        val result = item.entries.stream()
                .map({ x ->
                    val row = (x.value as LinkedHashMap<String,*>)

                    if (row is LinkedHashMap<*,*>) {
                        checkAndFlatMapStruct(row as LinkedHashMap<String, Any>, allTableInfo)
                    } else {
                        row
                    }
                })
                .collect(Collectors.toList<Any?>())

        return result
    }

    open fun checkAndFlatMapStruct(toAdd: LinkedHashMap<String, *>, allTableInfo: LinkedHashMap<String, TableInfo>): Any {
        val toAdd2: LinkedHashMap<String, Any?> = toAdd.clone() as LinkedHashMap<String, Any?>
        for ((key, itemNest) in toAdd) {
            if (itemNest is LinkedHashMap<*,*>) {
                if (itemNest.keys.isEmpty()) {
                    toAdd2[key] = null
                }
                else if (itemNest.keys.first().toString().matches(Regex("^\\d+$"))) {
                    toAdd2[key] = flatMapStruct(itemNest as LinkedHashMap<String,*>, allTableInfo)
                } else {
                    toAdd2[key] = checkAndFlatMapStruct(itemNest as LinkedHashMap<String,*>, allTableInfo)
                }
            } else {
                if (key == "_tbl") {
                    val handler = allTableInfo[toAdd.get("_tbl")]!!.dataClassHandler
                    if (handler != null) {
                        return handler(toAdd)
                    }
                    return toAdd
                }
            }
        }
        return toAdd2
    }


    open fun mapStruct(mapConf: MapperConfig, arr: List<Any>, delimiter: String = "-"): ArrayList<Any> {
        return mapStruct(mapConf.toTableInfo(), arr)
    }

    open fun mapStruct(allTableInfo: LinkedHashMap<String, TableInfo>, arr: List<Any>, delimiter: String = "-"): ArrayList<Any> {
        if (arr.size == 0) {
            return arrayListOf()
        }
        val rs = mapRaw(allTableInfo, arr, delimiter, true, false)
        //get the root table result since everything now is nested into objects.
        if (rs != null && rs.size > 0) {
            val rootTable = rs.values.first() as LinkedHashMap<String, LinkedHashMap<String, Any>>
            val arrFinal = ArrayList<Any>()

            var rootTblInfo: TableInfo? = null
            for ((tblAlias, tblInfo) in allTableInfo) {
                rootTblInfo = tblInfo
                break
            }
            for ((key, item) in rootTable) {
                var toAdd: Any = item
                toAdd = checkAndFlatMapStruct(toAdd as LinkedHashMap<String, Any>, allTableInfo) as Map<String, Any?>

                val handler = rootTblInfo?.dataClassHandler
                if (handler != null) {
                    arrFinal.add(handler(toAdd))
                }
            }
            return arrFinal
        }
        return arrayListOf()
    }

    open fun convertToMap(oriData: Any?): LinkedHashMap<String, Any?>? {
        var dataNew = LinkedHashMap<String, Any?>()

        if (oriData is Map<*, *>) {
            dataNew = oriData as LinkedHashMap<String, Any?>
        }
        return dataNew
    }
}
