package com.zeko.example

import com.github.jasync.sql.db.ResultSet
import com.github.jasync.sql.db.mysql.MySQLConnectionBuilder
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import com.zeko.model.*
import com.zeko.model.Select
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.core.json.JsonArray
import io.vertx.ext.asyncsql.MySQLClient
import org.joda.time.*
import java.util.LinkedHashMap
import org.joda.time.base.BaseLocal

class MainVerticle : CoroutineVerticle() {

    override suspend fun start() {
        val router = Router.router(vertx)
        router.get("/jasync-raw-sql").coroutineHandler { ctx -> jasyncRawSql(ctx) }
        router.get("/jasync-easy").coroutineHandler { ctx -> jasyncEasy(ctx) }
        router.get("/common-sql").coroutineHandler { ctx -> commonSql(ctx) }

        vertx.createHttpServer()
            .requestHandler(router)
            .listen(8080)
    }

    suspend fun jasyncRawSql(ctx: RoutingContext) {
        val sql = """
            select
                user.id as `user-id`,
                user.name as `user-name`,
                role.id as `role-id`,
                user.id as `role-user_id`,
                role.role_name as `role-role_name`,
                address.id as `address-id`,
                user.id as `address-user_id`,
                address.street1 as `address-street1`,
                address.street2 as `address-street2`
            from user
            left join address on address.user_id = user.id
            left outer join user_has_role on user_has_role.user_id = user.id
            left outer join role on role.id = user_has_role.role_id
        """.trimIndent()

        // Connection to MySQL DB
        val connection = MySQLConnectionBuilder.createConnectionPool("jdbc:mysql://localhost:3306/zeko_test?user=root&password=root")
        val resFuture = connection.sendPreparedStatement(sql)
        val queryResult = resFuture.get()
        connection.disconnect().get()

        val rows = queryResult.rows
        val results = rows.toMaps(listOf("user-id", "user-name", "role-id", "role-user_id", "role-role_name", "address-id", "address-user_id", "address-street1", "address-street2"))

        val tables = linkedMapOf<String, TableInfo>()
        tables["user"] = TableInfo(key = "id")
        tables["role"] = TableInfo(key = "id", move_under = "user", foreign_key = "user_id", many_to_many = true, remove = listOf("user_id"))
        tables["address"] = TableInfo(key = "id", move_under = "user", foreign_key = "user_id", many_to_one = true, remove = listOf("user_id"))

        val mapper = DataMapper()
        val mappedResults = mapper.map(tables, results)
        val json = Json.encodePrettily(mappedResults)

        ctx.response().end(json)
    }

    suspend fun jasyncEasy(ctx: RoutingContext) {
        val select = Select()
            .table("user").fields("id", "name")
            .table("role").fields("id", "role_name", "user.id = user_id")
            .table("address").fields("id", "street1", "street2", "user.id = user_id")

        val (columns, sqlFields) = select.prepare()

        val sql = """
            select $sqlFields
            from user
            left join address on address.user_id = user.id
            left outer join user_has_role on user_has_role.user_id = user.id
            left outer join role on role.id = user_has_role.role_id
        """.trimIndent()

        // Connection to MySQL DB
        val connection = MySQLConnectionBuilder.createConnectionPool("jdbc:mysql://localhost:3306/zeko_test?user=root&password=root")
        val resFuture = connection.sendPreparedStatement(sql)
        val queryResult = resFuture.get()
        connection.disconnect().get()

        val rows = queryResult.rows
        val results = rows.toMaps(columns)

        val tables = MapperConfig("id", true)
            .table("user")
            .table("role").manyToMany(true).moveUnder("user").foreignKey("user_id")
            .table("address").manyToOne(true).moveUnder("user").foreignKey("user_id")

        val mapper = DataMapper()
        val mappedResults = mapper.map(tables, results)
        val json = Json.encodePrettily(mappedResults)

        ctx.response().end(json)
    }


    suspend fun commonSql(ctx: RoutingContext) {
        val clientConf = JsonObject().put("host", "localhost")
            .put("port", 3306)
            .put("database", "zeko_test")
            .put("username", "root")
            .put("password", "root")

        val client = MySQLClient.createShared(vertx, clientConf)

        val select = Select()
            .table("user").fields("id", "name")
            .table("role").fields("id", "role_name", "user.id = user_id")
            .table("address").fields("id", "street1", "street2", "user.id = user_id")

        val (columns, sqlFields) = select.prepare()

        val sql = """
            select $sqlFields
            from user
            left join address on address.user_id = user.id
            left outer join user_has_role on user_has_role.user_id = user.id
            left outer join role on role.id = user_has_role.role_id
        """.trimIndent()

        client.getConnection { res ->
            if (res.succeeded()) {
                val connection = res.result()

                connection.query(sql, {
                    val rows = it.result().results
                    val results = rows.toMaps(columns)

                    val tables = MapperConfig("id", true)
                        .table("user")
                        .table("role").manyToMany(true).moveUnder("user").foreignKey("user_id")
                        .table("address").manyToOne(true).moveUnder("user").foreignKey("user_id")

                    val mapper = DataMapper()
                    val mappedResults = mapper.map(tables, results)

                    val json = Json.encodePrettily(mappedResults)
                    ctx.response().end(json)
                    connection.close()
                })
            }
        }
    }

    /**
     * An extension method for simplifying coroutines usage with Vert.x Web routers
     */
    fun Route.coroutineHandler(fn: suspend (RoutingContext) -> Unit) {
        handler { ctx ->
            launch(ctx.vertx().dispatcher()) {
                try {
                    fn(ctx)
                } catch (e: Exception) {
                    ctx.fail(e)
                }
            }
        }
    }

    /**
     * Extension method to Jasync ResultSet toMaps
     */
    fun ResultSet.toMaps(sel: SelectInfo, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
        return ResultSetHelper.toMaps(this, sel.columns, timeProcessor, tzFrom, tzTo)
    }

    fun ResultSet.toMaps(columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
        return ResultSetHelper.toMaps(this, columns, timeProcessor, tzFrom, tzTo)
    }

    /**
     * Extension method to vertx common-sql results which returns List<JsonArray> in a query
     */
    fun List<JsonArray>.toMaps(sel: SelectInfo, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
        return ResultSetHelper.toMaps(this, sel.columns, timeProcessor, tzFrom, tzTo)
    }

    fun List<JsonArray>.toMaps(columns: List<String>, timeProcessor: ((BaseLocal, DateTimeZone?, DateTimeZone?) -> Any)? = null, tzFrom: DateTimeZone? = null, tzTo: DateTimeZone? = null): List<LinkedHashMap<String, Any?>> {
        return ResultSetHelper.toMaps(this, columns, timeProcessor, tzFrom, tzTo)
    }
}
