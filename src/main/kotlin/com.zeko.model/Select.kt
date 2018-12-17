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

import java.util.LinkedHashMap

data class SelectInfo(val columns: List<String>, val sqlFields: String)

class Select {
    private val fieldsToSelect by lazy {
        LinkedHashMap<String, Array<String>>()
    }
    private lateinit var currentTable: String
    private var espChar: String
    private var asChar: String

    constructor(espChar: String =  "`", asChar: String = "=") {
        this.espChar = espChar
        this.asChar = asChar
    }

    fun table(name: String): Select {
        currentTable = name
        return this
    }

    fun fields(vararg names: String): Select {
        if (!currentTable.isNullOrEmpty()) {
            fieldsToSelect[currentTable] = names as Array<String>
        }
        return this
    }

    fun prepare(): SelectInfo {
        val selectFields = mutableListOf<String>()
        val columns = mutableListOf<String>()

        for ((tbl, cols) in fieldsToSelect) {
            for (colName in cols) {
                if (colName.indexOf("=") != -1) {
                    val parts = colName.split(asChar)
                    val tblLinkedCol = parts[0].trim()
                    val selfCol = parts[1].trim()

                    val aliasName = "$tbl-$selfCol"
                    columns.add(aliasName)
                    selectFields.add("$tblLinkedCol as $espChar$aliasName$espChar")
                } else {
                    val aliasName = "$tbl-$colName"
                    columns.add(aliasName)
                    selectFields.add("$tbl.$colName as $espChar$aliasName$espChar")
                }
            }
        }

        val sqlFields = selectFields.joinToString(", ")
        return SelectInfo(columns, sqlFields)
    }
}
