package com.zeko.model

import io.vertx.core.json.JsonArray
import java.util.LinkedHashMap
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe
import org.spekframework.spek2.style.gherkin.Feature
import kotlin.reflect.KVisibility
import kotlin.reflect.full.memberProperties
import kotlin.test.assertEquals
import kotlin.test.assertTrue

open class Entity(map: Map<String, Any?>) {
    protected val defaultMap = map.withDefault { null }

    override fun toString(): String {
        var str = this::class.java.toString() + "{ "
        this::class.memberProperties.forEach { member ->
            if (member.visibility == KVisibility.PUBLIC) {
                str += "${member.name}-> ${member.getter.call(this)}, "
            }
        }
        return str.removeSuffix(", ") + " }"
    }
}

class User(map: Map<String, Any?>): Entity(map) {
    val id: Int?     by defaultMap
    val name: String? by defaultMap
    val role_id: Int? by defaultMap
    val role: List<Role>? by defaultMap
    val address: List<Address>? by defaultMap
}

class Role(map: Map<String, Any?>): Entity(map) {
    val id: Int?     by defaultMap
    val role_name: String? by defaultMap
    val user_id: Int? by defaultMap
}


class Address(map: Map<String, Any?>): Entity(map) {
    val id: Int?     by defaultMap
    val user_id: Int? by defaultMap
    val street1: String? by defaultMap
    val street2: String? by defaultMap
}


class DataMapperPOJOSpec : Spek({

    describe("A query result with two table joins (1:m and m:m), user has multiple addresses and has many roles") {

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
        table["user"] = TableInfo(key = "id", mapClass = User::class.java)
        table["role"] = TableInfo(key = "id", move_under = "user", foreign_key = "user_id", many_to_many = true, mapClass = Role::class.java)
        table["address"] = TableInfo(key = "id", move_under = "user", foreign_key = "user_id", many_to_one = true, remove = listOf("user_id"), mapClass = Address::class.java)

        val mapper = DataMapper()
        val result = mapper.mapStruct(table, all) as List<User>

        context("mapping of result") {
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

                it("should be User instances") {
                    assertTrue(User::class.java == result[0]::class.java)
                    assertTrue(User::class.java == result[1]::class.java)
                }

                it("the first object in the array mapped should have all the user fields selected") {
                    assertEquals(1, result[0].id)
                    assertEquals("Leng", result[0].name)
                    assertEquals(2, result[0].role_id)
                }

                val roles = result[0].role

                it("should also have a list of nested role objects") {
                    assertEquals(true, roles != null)
                }

                if (roles != null) {
                    it("should have one role") {
                        assertEquals(true, roles.size == 1)
                    }

                    val role = roles[0]

                    it("should be a Role instance") {
                        assertTrue(Role::class.java == role::class.java)
                    }

                    it("should have role name, role id, user id matched") {
                        assertEquals(2, role.id)
                        assertEquals("Super Admin", role.role_name)
                        assertEquals(1, role.user_id)
                    }
                }


                val addresses = result[0].address

                it("should also have a list of nested address objects") {
                    assertEquals(true, addresses != null)
                }

                if (addresses != null) {
                    it("should have 2 addresses") {
                        assertEquals(true, addresses.size == 2)
                    }

                    it("should be Address instances") {
                        assertTrue(Address::class.java == addresses[0]::class.java)
                        assertTrue(Address::class.java == addresses[1]::class.java)
                    }

                    it("should have 1st address street1 and street2 matched") {
                        assertEquals(128, addresses[0].id)
                        assertEquals("Some block", addresses[0].street1)
                        assertEquals("in the street", addresses[0].street2)
                    }

                    it("should have 2nd address street1 and street2 matched") {
                        assertEquals(129, addresses[1].id)
                        assertEquals("Company Block", addresses[1].street1)
                        assertEquals("in the CBD", addresses[1].street2)
                    }
                }

                // for 2nd user object
                it("the 2nd object in the array mapped should have all the user fields selected") {
                    assertEquals(2, result[1].id)
                    assertEquals("Superman", result[1].name)
                    assertEquals(2, result[1].role_id)
                }

                val roles2 = result[1].role

                it("should also have a list of nested role objects") {
                    assertEquals(true, roles2 != null)
                }

                if (roles2 != null) {
                    it("should have one role") {
                        assertEquals(true, roles2.size == 1)
                    }

                    val role2 = roles2[0]

                    it("should be a Role instance") {
                        assertTrue(Role::class.java == role2::class.java)
                    }

                    it("should have role name, role id, user id matched") {
                        assertEquals(2, role2.id)
                        assertEquals("Super Admin", role2.role_name)
                        assertEquals(2, role2.user_id)
                    }
                }


                val addresses2 = result[1].address

                it("should also have a list of nested address objects") {
                    assertEquals(true, addresses2 != null)
                }

                if (addresses2 != null) {
                    it("should have 1 address") {
                        assertEquals(true, addresses2.size == 1)
                    }

                    it("should be Address instances") {
                        assertTrue(Address::class.java == addresses2[0]::class.java)
                    }

                    it("should have 1st address street1 and street2 matched") {
                        assertEquals(131, addresses2[0].id)
                        assertEquals("A capsule", addresses2[0].street1)
                        assertEquals("in the yard", addresses2[0].street2)
                    }
                }
            }
        }

    }
})
