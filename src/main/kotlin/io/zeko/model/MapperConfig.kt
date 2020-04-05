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

class MapperConfig(defaultPrimaryKey: String, autoRemoveLinkKey: Boolean) {

    var tableInfo = LinkedHashMap<String, TableInfo>()
    var latestTable = ""
    var defaultPrimaryKey = defaultPrimaryKey
    var autoRemoveLinkKey = autoRemoveLinkKey

    fun defaultPrimaryKeyTo(defaultPrimaryKey: String): MapperConfig {
        this.defaultPrimaryKey = defaultPrimaryKey
        return this
    }

    fun shouldRemoveLinkKey(autoRemoveLinkKey: Boolean): MapperConfig {
        this.autoRemoveLinkKey = autoRemoveLinkKey
        return this
    }

    fun table(tableName: String): MapperConfig {
        return table(tableName, null)
    }

    fun table(tableName: String, defaultPrimaryKey: String?): MapperConfig {
        this.tableInfo[tableName] = TableInfo("id", null, null, null, false, false, false, false, false, null)
        this.latestTable = tableName
        if (defaultPrimaryKey != null) {
            this.primaryKey(defaultPrimaryKey)
        }
        else if (this.defaultPrimaryKey != null) {
            this.primaryKey(this.defaultPrimaryKey)
        }
        return this
    }

    fun currentTable(): TableInfo {
        return this.tableInfo[this.latestTable] as TableInfo
    }

    fun removeLinkKey(): MapperConfig {
        this.remove(this.currentTable().foreign_key as String)
        return this
    }

    fun remove(fieldName: Any): MapperConfig {
        val table = this.currentTable()
        if (table.remove == null) {
            table.remove = ArrayList<String>()
        }
        if (fieldName is List<*>) {
            (table.remove as ArrayList).addAll((fieldName as List<String>))
        } else if (fieldName is String) {
            if (!(table.remove as ArrayList).contains(fieldName)) {
                (table.remove as ArrayList).add(fieldName)
            }
        }
        return this
    }

    fun primaryKey(fieldName: String): MapperConfig {
        val table = this.currentTable()
        table.key = fieldName
        return this
    }

    fun rename(tableName: String): MapperConfig {
        val table = this.currentTable()
        table.rename = tableName
        return this
    }

    fun foreignKey(fieldName: String): MapperConfig {
        val table = this.currentTable()
        table.foreign_key = fieldName
        if (this.autoRemoveLinkKey) {
            return this.removeLinkKey()
        }
        return this
    }

    fun oneToOne(bool: Boolean): MapperConfig {
        val table = this.currentTable()
        table.one_to_one = bool
        return this
    }

    fun manyToMany(bool: Boolean): MapperConfig {
        val table = this.currentTable()
        table.many_to_many = bool
        return this
    }

    fun oneToMany(bool: Boolean): MapperConfig {
        val table = this.currentTable()
        table.one_to_many = bool
        return this
    }

    fun manyToOne(bool: Boolean): MapperConfig {
        val table = this.currentTable()
        table.many_to_one = bool
        return this
    }

    fun moveUnder(fieldName: String): MapperConfig {
        val table = this.currentTable()
        table.move_under = fieldName
        return this
    }

    fun mapTo(mapClass: Class<*>): MapperConfig {
        val table = this.currentTable()
        table.mapClass = mapClass
        return this
    }

    fun toArrayMap(): LinkedHashMap<String, TableInfo> {
        this.tableInfo = this.sortTableInfo(this.tableInfo)
        return this.tableInfo
    }

    fun toTableInfo(): LinkedHashMap<String, TableInfo> {
        this.tableInfo = this.sortTableInfo(this.tableInfo)
        return this.tableInfo
    }

    fun sortTableInfo(tables: LinkedHashMap<String, TableInfo>): LinkedHashMap<String, TableInfo> {
        val tableList = tables.keys.asIterable()
        val firstTable = tableList.first()
        var selfTblIndex = -1

        for (tbl in tableList) {
            selfTblIndex++
            var moveUnderTbl = tables[tbl]?.move_under

            //no need to sort if linked table is the root table
            if (moveUnderTbl != null && moveUnderTbl != firstTable) {
                if (tables[tbl]?.rename != null) {
                    moveUnderTbl = tables[tbl]?.rename
                }

                val linkTblIndex = tableList.indexOf(moveUnderTbl) //\array_search(moveUnderTbl, tableList)

                //if move_under table was before the table(self) to link to, then resort, move it before
                if (selfTblIndex > linkTblIndex && linkTblIndex > -1) {
                    //first table remains the same
                    val tables2 = LinkedHashMap<String, TableInfo>()
                    tables2[tbl] = tables[tbl] as TableInfo
                    tables2.putAll(tables)

                    val tables3 = LinkedHashMap<String, TableInfo>()
                    tables3[firstTable] = tables[firstTable] as TableInfo
                    tables3.putAll(tables2)

                    return this.sortTableInfo(tables3)
                }
            }
        }
        return tables
    }
}
