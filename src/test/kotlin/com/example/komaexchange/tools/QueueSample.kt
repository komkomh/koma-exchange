package com.example.komaexchange.tools

import java.util.concurrent.ConcurrentLinkedQueue

private val queue = ConcurrentLinkedQueue<String>()

fun main() {
    queue.add("hello1")
    queue.add("hello2")

    poll()
    poll()
    poll()
}

fun poll() {
    val value: String? = queue.poll()
    if (value == null) {
        println("xx is null")
    }
    println("size = ${queue.size} value = $value")
}