package com.example.komaexchange.tools

import com.example.komaexchange.entities.Order
import java.math.BigDecimal
import java.util.*

fun main() {

    val order1 = createOrder(1L)
    val order2 = createOrder(1L)
    println("equals = ${order1 == order2}")
    println("hashCode = ${order1.hashCode()}")
    println("hashCode = ${order2.hashCode()}")
    println("hashCodeEquals = ${order1.hashCode() == order2.hashCode()}")

    val orders = sortedSetOf<Order>(order1, order2)
    println("orders.size = ${orders.size}")
    val hashOrders = setOf<Order>(order1, order2)
    println("hashOrders.size = ${hashOrders.size}")

    // 条件
    val orders1 = sortedSetOf<Order>(
        createOrder(2L),
        createOrder(1L),
    )
    val orders2 = TreeSet(orders1.map { it.copy() })

    // 操作
    orders2.remove(createOrder(1L))
    orders2.add(createOrder(2L).copy(amount = BigDecimal(99)))
    orders2.add(createOrder(3L))

    println("contain = ${orders2.contains(createOrder(3L))}")

    // 検証
    println("---------- order1")
    orders1.forEach { println(it) }

    println("---------- order2")
    orders2.forEach { println(it) }
}


