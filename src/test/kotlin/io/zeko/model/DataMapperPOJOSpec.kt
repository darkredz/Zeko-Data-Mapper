package io.zeko.model

import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import java.util.LinkedHashMap
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe
import org.spekframework.spek2.style.gherkin.Feature
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class User : Entity {
    constructor(map: Map<String, Any?>) : super(map)
    constructor(vararg props: Pair<String, Any?>) : super(*props)
    var id: Int?     by map
    var name: String? by map
    var roleId: Int? by map
    var role: List<Role>? by map
    var address: List<Address>? by map
    var customerData: CustomerData? by map
}

class Role : Entity {
    constructor(map: Map<String, Any?>) : super(map)
    constructor(vararg props: Pair<String, Any?>) : super(*props)
    val id: Int?     by map
    val roleName: String? by map
    val userId: Int? by map
}

class Address : Entity {
    constructor(map: Map<String, Any?>) : super(map)
    constructor(vararg props: Pair<String, Any?>) : super(*props)
    var id: Int?     by map
    var userId: Int? by map
    var street1: String? by map
    var street2: String? by map
}

class CustomerData : Entity {
    constructor(map: Map<String, Any?>) : super(map)
    constructor(vararg props: Pair<String, Any?>) : super(*props)
    val id: Int?     by map
    var userId: Int? by map
    val totalSpent: Double? by map
    val refund: List<Refund>? by map
}

class Refund : Entity {
    constructor(map: Map<String, Any?>) : super(map)
    constructor(vararg props: Pair<String, Any?>) : super(*props)
    val id: Int?     by map
    val customerDataId: Int?     by map
    val itemName: String? by map
    val quantity: Int? by map
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
            "address-street2" to "in the street",
            "customer_data-id" to 99,
            "customer_data-user_id" to 1,
            "customer_data-total_spent" to 58209.50,
            "refund-id" to 150,
            "refund-item_name" to "Product One",
            "refund-quantity" to 10,
            "refund-customer_data_id" to 150
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
            "address-street2" to "in the CBD",
            "customer_data-id" to 99,
            "customer_data-user_id" to 1,
            "customer_data-total_spent" to 58209.50,
            "refund-id" to 150,
            "refund-item_name" to "Product One",
            "refund-quantity" to 10,
            "refund-customer_data_id" to 150
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
            "address-street2" to "in the yard",
            "customer_data-id" to 100,
            "customer_data-user_id" to 2,
            "customer_data-total_spent" to 88.00,
        ))

        all.add(linkedMapOf(
            "user-id" to 3,
            "user-name" to "Batman",
            "user-role_id" to 3,
            "role-id" to 3,
            "role-role_name" to "User",
            "role-user_id" to 3,
            "address-id" to 133,
            "address-user_id" to 3,
            "address-street1" to "Market St",
            "address-street2" to "Some where",
            "customer_data-id" to 125,
            "customer_data-user_id" to 3,
            "customer_data-total_spent" to 678.30,
            "refund-id" to 150,
            "refund-item_name" to "Product One",
            "refund-quantity" to 10,
            "refund-customer_data_id" to 125,
        ))


        all.add(linkedMapOf(
            "user-id" to 3,
            "user-name" to "Batman",
            "user-role_id" to 3,
            "role-id" to 3,
            "role-role_name" to "User",
            "role-user_id" to 3,
            "address-id" to 133,
            "address-user_id" to 3,
            "address-street1" to "Market St",
            "address-street2" to "Some where",
            "customer_data-id" to 125,
            "customer_data-user_id" to 3,
            "customer_data-total_spent" to 678.30,
            "refund-id" to 151,
            "refund-item_name" to "Product OneDotOne",
            "refund-quantity" to 11,
            "refund-customer_data_id" to 125,
        ))

        all.add(linkedMapOf(
            "user-id" to 4,
            "user-name" to "Joker",
            "user-role_id" to 3,
            "role-id" to 3,
            "role-role_name" to "User",
            "role-user_id" to 4,
            "address-id" to 155,
            "address-user_id" to 4,
            "address-street1" to "Market St",
            "address-street2" to "Some where",
            "customer_data-id" to 177,
            "customer_data-user_id" to 4,
            "customer_data-total_spent" to 112.00,
            "refund-id" to 151,
            "refund-item_name" to "Product MMM",
            "refund-quantity" to 22,
            "refund-customer_data_id" to 177,
        ))

        val table = linkedMapOf<String, TableInfo>()
        table["user"] = TableInfo(key = "id", dataClassHandler = { User(it) })
        table["role"] = TableInfo(key = "id", move_under = "user", foreign_key = "user_id", many_to_many = true, dataClassHandler = { Role(it) })
        table["address"] = TableInfo(key = "id", move_under = "user", foreign_key = "user_id", many_to_one = true, remove = listOf("user_id"), dataClassHandler = { Address(it) })
        table["refund"] = TableInfo(key = "id", move_under = "customer_data", foreign_key = "customer_data_id", many_to_one = true, dataClassHandler = { Refund(it) })
        table["customer_data"] = TableInfo(key = "id", move_under = "user", foreign_key = "user_id", one_to_one = true, remove = listOf("user_id"), dataClassHandler = { CustomerData(it) })

        val mapper = DataMapper()
        val result = mapper.mapStruct(table, all) as List<User>

        println(Json.encodePrettily(result))

        context("mapping of result") {
            it("should not be null") {
                assertEquals(false, result == null)
            }

            if (result != null) {
                it("should not be empty") {
                    assertEquals(false, result.isEmpty())
                }
                it("should have 4 elements") {
                    assertEquals(4, result.size)
                }

                it("should be User instances") {
                    assertTrue(User::class.java == result[0]::class.java)
                    assertTrue(User::class.java == result[1]::class.java)
                    assertTrue(User::class.java == result[2]::class.java)
                }

                it("the first object in the array mapped should have all the user fields selected") {
                    assertEquals(1, result[0].id)
                    assertEquals("Leng", result[0].name)
                    assertEquals(2, result[0].roleId)
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
                        assertEquals("Super Admin", role.roleName)
                        assertEquals(1, role.userId)
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
                    assertEquals(2, result[1].roleId)
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
                        assertEquals("Super Admin", role2.roleName)
                        assertEquals(2, role2.userId)
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
