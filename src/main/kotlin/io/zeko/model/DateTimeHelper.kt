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

import org.joda.time.DateTimeZone
import org.joda.time.LocalDateTime
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import org.joda.time.DateTime
import org.joda.time.base.BaseLocal
import org.joda.time.format.ISODateTimeFormat

class DateTimeHelper {
    companion object {
        fun toDateTimeStrUTC(value: BaseLocal): Any {
            val dt = when (value) {
                is LocalDateTime -> value.toString(ISODateTimeFormat.dateTime()) + "Z"
                is LocalDate -> value.toString(ISODateTimeFormat.date())
                is LocalTime -> value.toString(ISODateTimeFormat.time())
                else -> value
            }
            return dt
        }

        fun toDateTimeStrZone(value: BaseLocal, tzFrom: DateTimeZone?, tzTo: DateTimeZone?): Any {
            val dt = when (value) {
                is LocalDateTime -> DateTime(value.toDateTime(tzFrom).millis).toDateTime(tzTo).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
                is LocalDate -> value.toString(ISODateTimeFormat.date())
                is LocalTime -> value.toString(ISODateTimeFormat.time())
                else -> value
            }
            return dt
        }

        fun toUnixTimeMilis(value: BaseLocal): Any {
            val dt = when (value) {
                is LocalDateTime -> value.toDateTime(DateTimeZone.UTC).millis
                else -> value
            }
            return dt
        }

        fun toUnixTime(value: BaseLocal): Any {
            val milis = toUnixTimeMilis(value)
            if (milis is Long) {
                return milis / 1000
            }
            return milis
        }
    }
}
