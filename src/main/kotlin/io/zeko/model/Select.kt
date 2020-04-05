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

import java.util.LinkedHashMap

data class SelectInfo(val columns: List<String>, val sqlFields: String)

open class Select {
    protected val fieldsToSelect by lazy {
        LinkedHashMap<String, Array<String>>()
    }
    protected var currentTable: String = ""

    var espChar: String
        get() = field
    var asChar: String
        get() = field
    var espTableName: Boolean
        get() = field

    constructor(espChar: String =  "`", asChar: String = "=", espTableName: Boolean = false) {
        this.espChar = espChar
        this.asChar = asChar
        this.espTableName = espTableName
    }

    constructor(espChar: String =  "`", espTableName: Boolean = false) {
        this.espChar = espChar
        this.asChar = "="
        this.espTableName = espTableName
    }

    open fun table(name: String): Select {
        currentTable = name
        return this
    }

    open fun fields(vararg names: String): Select {
        fieldsToSelect[currentTable] = names as Array<String>
        return this
    }

    fun prepare(): SelectInfo {
        val selectFields = mutableListOf<String>()
        val columns = mutableListOf<String>()

        for ((tbl, cols) in fieldsToSelect) {
            for (colName in cols) {
                if (colName.indexOf("=") != -1) {
                    val parts = colName.split(asChar)
                    val partField = parts[0].trim()
                    var tblLinkedCol: String
                    if (!espTableName) {
                        tblLinkedCol = partField
                    } else {
                        val fieldParts = partField.split(".")
                        val tblLinked = fieldParts[0]
                        tblLinkedCol = "${espChar}${tblLinked}${espChar}.${fieldParts[1]}"
                    }
                    val selfCol = parts[1].trim()
                    if (tbl == "") {
                        selectFields.add("$colName")
                    } else {
                        val aliasName = "$tbl-$selfCol"
                        columns.add(aliasName)
                        selectFields.add("$tblLinkedCol as $espChar$aliasName$espChar")
                    }
                } else {
                    if (tbl == "") {
                        selectFields.add("$colName")
                    } else {
                        val aliasName = "$tbl-$colName"
                        columns.add(aliasName)
                        val tblFinal = if (espTableName) "$espChar$tbl$espChar" else tbl
                        selectFields.add("$tblFinal.$colName as $espChar$aliasName$espChar")
                    }
                }
            }
        }

        val sqlFields = selectFields.joinToString(", ")
        return SelectInfo(columns, sqlFields)
    }
}
