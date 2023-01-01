package com.example.komaexchange.tools

import com.example.komaexchange.entities.Order
import java.math.BigDecimal
import java.util.TreeSet


fun main() {
    val orders1: MutableSet<Order> = mutableSetOf(createOrder(1L).copy(amount = BigDecimal(1)))
    val orders2: MutableSet<Order> = mutableSetOf(createOrder(1L).copy(amount = BigDecimal(2)))

    orders1.removeAll(orders2)
    orders1.addAll(orders2)
    println(orders1)
}

