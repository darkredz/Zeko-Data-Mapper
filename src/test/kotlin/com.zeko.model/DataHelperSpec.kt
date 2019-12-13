package com.zeko.model

import com.caucho.quercus.env.Env
import com.caucho.quercus.QuercusContext
import com.caucho.quercus.env.ArrayValue
import java.util.LinkedHashMap
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe
import org.spekframework.spek2.style.gherkin.Feature
import kotlin.test.assertEquals

class DataHelperSpec : Spek({

    describe("An array of 2 HashMap objects") {
        val all = ArrayList<LinkedHashMap<String, Any>>()
        all.add(linkedMapOf(
                "id" to 123,
                "name" to "Leng",
                "age" to 12,
                "credit" to 856.87
        ))

        all.add(linkedMapOf(
                "id" to 124,
                "name" to "Superman",
                "age" to 55,
                "credit" to 986.08
        ))

        context("convert result to json array") {
            val result = DataHelper.toJsonArray(all)
            it("should not be empty") {
                assertEquals(false, result.isEmpty)
            }
            it("should have 2 elements") {
                val size = result.size()
                assertEquals(2, size)
            }
            it("should object with all the fields set") {
                assertEquals(123, result.getJsonObject(0).getInteger("id"))
                assertEquals("Leng", result.getJsonObject(0).getString("name"))
                assertEquals(12, result.getJsonObject(0).getInteger("age"))
                assertEquals(856.87, result.getJsonObject(0).getDouble("credit"))
            }
        }

        context("convert result to PHP array") {
            val quercusContext = QuercusContext()
            val env = Env(quercusContext)
            val result = DataHelper.toPhpArray(env, all)

            it("should not be null") {
                assertEquals(false, result == null)
            }
            it("should not be empty") {
                assertEquals(false, result.isEmpty)
            }
            it("should have 2 element") {
                assertEquals(2, result.size, "Got " + result.size)
            }
            it("and should object with field id, name, age") {
                val obj = result.get(0) as ArrayValue
                val map = obj.toJavaMap(env, HashMap::class.java)
                assertEquals(123, map.get("id"))
                assertEquals("Leng", map.get("name"))
                assertEquals(12, map.get("age"))
                assertEquals(856.87, map.get("credit"))
            }
        }
    }
})
