package com.example.komaexchange.tools

import java.util.concurrent.ConcurrentLinkedQueue

private val queue = ConcurrentLinkedQueue<String>()

fun main() {
    queue.add("hello1")
    queue.add("hello2")

    call()
    call()
    call()
    queue.poll()
}

fun call() {
    val value: String? = queue.peek()
    if (value == null) {
        println("xx is null")
    }
    println("size = ${queue.size} value = $value")
}