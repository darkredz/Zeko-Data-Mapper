package com.zeko.model

import java.util.LinkedHashMap
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class DataMapperSpec : Spek({

    given("A query result with two table joins (1:m and m:m), user has multiple addresses and has many roles") {

        val all = ArrayList<LinkedHashMap<String, Any>>()
        all.add(linkedMapOf(
                "user-id" to 1,
                "user-name" to "Leng",
                "user-role_id" to 2,
                "role-id" to 2,
                "role-role_name" to "Super Admin",
                "role-user_id" to 1,   //this is selected as alias, not actual field in table. To be used with the mapper
                "address-id" to 128,
                "address-user_id" to 1,
                "address-street1" to "Some block",
                "address-street2" to "in the street"
        ))

        all.add(linkedMapOf(
                "user-id" to 1,
                "user-name" to "Leng",
                "user-role_id" to 2,
                "role-id" to 2,
                "role-role_name" to "Super Admin",
                "role-user_id" to 1,
                "address-id" to 129,
                "address-user_id" to 1,
                "address-street1" to "Company Block",
                "address-street2" to "in the CBD"
        ))

        all.add(linkedMapOf(
                "user-id" to 2,
                "user-name" to "Superman",
                "user-role_id" to 2,
                "role-id" to 2,
                "role-role_name" to "Super Admin",
                "role-user_id" to 2,
                "address-id" to 131,
                "address-user_id" to 2,
                "address-street1" to "A capsule",
                "address-street2" to "in the yard"
        ))

        val table = linkedMapOf<String, TableInfo>()
        table["user"] = TableInfo(key = "id")
        table["role"] = TableInfo(key = "id", move_under = "user", foreign_key = "user_id", many_to_many = true)
        table["address"] = TableInfo(key = "id", move_under = "user", foreign_key = "user_id", many_to_one = true, remove = listOf("user_id"))

        val mapper = DataMapper()
        val result = mapper.map(table, all)

        on("mapping of result") {
            it("should not be null") {
                assertEquals(false, result == null)
            }

            if (result != null) {
                it("should not be empty") {
                    assertEquals(false, result.isEmpty())
                }
                it("should have 2 elements") {
                    assertEquals(2, result.size)
                }

                it("the first object in the array mapped should have all the user fields selected") {
                    assertEquals(1, result[0]["id"])
                    assertEquals("Leng", result[0]["name"])
                    assertEquals(2, result[0]["role_id"])
                }

                val roles = result[0]["role"] as List<LinkedHashMap<String, Any>>

                it("should also have a list of nested role objects") {
                    assertEquals(true, roles != null)
                }

                if (roles != null) {
                    it("should have one role") {
                        assertEquals(true, roles.size == 1)
                    }

                    val role = roles[0]

                    it("should have role name, role id, user id matched") {
                        assertEquals(2, role["id"])
                        assertEquals("Super Admin", role["role_name"])
                        assertEquals(1, role["user_id"])
                    }
                }


                val addresses = result[0]["address"] as List<LinkedHashMap<String, Any>>

                it("should also have a list of nested address objects") {
                    assertEquals(true, addresses != null)
                }

                if (addresses != null) {
                    it("should have 2 addresses") {
                        assertEquals(true, addresses.size == 2)
                    }

                    it("should have 1st address street1 and street2 matched") {
                        assertEquals(128, addresses[0]["id"])
                        assertEquals("Some block", addresses[0]["street1"])
                        assertEquals("in the street", addresses[0]["street2"])
                    }

                    it("should have 2nd address street1 and street2 matched") {
                        assertEquals(129, addresses[1]["id"])
                        assertEquals("Company Block", addresses[1]["street1"])
                        assertEquals("in the CBD", addresses[1]["street2"])
                    }
                }

                // for 2nd user object
                it("the 2nd object in the array mapped should have all the user fields selected") {
                    assertEquals(2, result[1]["id"])
                    assertEquals("Superman", result[1]["name"])
                    assertEquals(2, result[1]["role_id"])
                }

                val roles2 = result[1]["role"] as List<LinkedHashMap<String, Any>>

                it("should also have a list of nested role objects") {
                    assertEquals(true, roles2 != null)
                }

                if (roles2 != null) {
                    it("should have one role") {
                        assertEquals(true, roles2.size == 1)
                    }

                    val role2 = roles[0]

                    it("should have role name, role id, user id matched") {
                        assertEquals(2, role2["id"])
                        assertEquals("Super Admin", role2["role_name"])
                        assertEquals(2, role2["user_id"])
                    }
                }


                val addresses2 = result[1]["address"] as List<LinkedHashMap<String, Any>>

                it("should also have a list of nested address objects") {
                    assertEquals(true, addresses2 != null)
                }

                if (addresses2 != null) {
                    it("should have 1 address") {
                        assertEquals(true, addresses2.size == 1)
                    }

                    it("should have 1st address street1 and street2 matched") {
                        assertEquals(131, addresses2[0]["id"])
                        assertEquals("A capsule", addresses2[0]["street1"])
                        assertEquals("in the yard", addresses2[0]["street2"])
                    }
                }
            }
        }

    }
})
