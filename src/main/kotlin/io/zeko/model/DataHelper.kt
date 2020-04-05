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

import com.caucho.quercus.env.ArrayValue
import com.caucho.quercus.env.ArrayValueImpl
import com.caucho.quercus.env.Env
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.LinkedHashMap

class DataHelper {

    companion object {
        fun toPhpArray(env: Env, rs: List<LinkedHashMap<String, Any>>): ArrayValue {
            val arr = ArrayValueImpl()
            for (map in rs) {
                arr.append(env.wrapJava(map))
            }
            return arr
        }

        fun toJsonArray(rs: List<LinkedHashMap<String, Any>>): JsonArray {
            val arr = JsonArray()
            for (map in rs) {
                arr.add(JsonObject.mapFrom(map))
            }
            return arr
        }
    }
}