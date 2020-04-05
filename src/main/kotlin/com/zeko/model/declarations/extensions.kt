package com.zeko.model.declarations

import com.github.jasync.sql.db.ResultSet
import com.zeko.model.ResultSetHelper
import com.zeko.model.SelectInfo
import io.vertx.core.json.JsonArray
import org.joda.time.DateTimeZone
import org.joda.time.base.BaseLocal
import java.util.LinkedHashMap


fun io.vertx.ext.sql.ResultSet.toMaps(columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
    return ResultSetHelper.toMaps(this, columns, timeProcessor, tzFrom, tzTo)
}

fun io.vertx.ext.sql.ResultSet.toMaps(sel: SelectInfo, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
    return ResultSetHelper.toMaps(this, sel.columns, timeProcessor, tzFrom, tzTo)
}

fun java.sql.ResultSet.toMaps(columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
    return ResultSetHelper.toMaps(this, columns, timeProcessor, tzFrom, tzTo)
}

fun java.sql.ResultSet.toMaps(sel: SelectInfo, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
    return ResultSetHelper.toMaps(this, sel.columns, timeProcessor, tzFrom, tzTo)
}

fun ResultSet.toMaps(columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
    return ResultSetHelper.toMaps(this, columns, timeProcessor, tzFrom, tzTo)
}

fun ResultSet.toMaps(sel: SelectInfo, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
    return ResultSetHelper.toMaps(this, sel.columns, timeProcessor, tzFrom, tzTo)
}

fun List<JsonArray>.toMaps(columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
    return ResultSetHelper.toMaps(this, columns, timeProcessor, tzFrom, tzTo)
}

fun List<JsonArray>.toMaps(sel: SelectInfo, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
    return ResultSetHelper.toMaps(this, sel.columns, timeProcessor, tzFrom, tzTo)
}
