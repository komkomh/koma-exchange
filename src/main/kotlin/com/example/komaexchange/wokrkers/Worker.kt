package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.ShardMaster
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlin.reflect.KClass

abstract class Worker<T : Any>(
) {

    val shardMaster: ShardMaster = ShardMaster()
    val mutex = Mutex()
    private val queue: Queue<T> = Queue()

    abstract fun getEntityClazz(): KClass<T>;
    abstract fun new(): Worker<T>;
    abstract fun execute(t: T?): QueueOrder

    suspend fun receive() {

    }

    suspend fun consume() {

        var entity: T? = queue.peekWait()
        while (true) {
            entity = when (execute(entity)) {
                QueueOrder.CONTINUE -> {
                    queue.peek()
                }

                QueueOrder.DONE -> {
                    queue.done()
                    queue.peekWait()
                }

                QueueOrder.RESET -> {
                    queue.reset()
                    queue.peekWait()
                }

                QueueOrder.UNTIL_DONE -> {
                    queue.untilDone()
                    queue.peekWait()
                }
            }
        }
    }
}

class Queue<T : Any> {
    private val queue = mutableListOf<T>()
    private var peekCount = 0

    fun offer(t: T) {
        queue.add(t)
    }

    fun peek(): T? {
        return when (queue.size > peekCount) {
            true -> queue[peekCount++]
            false -> null
        }
    }

    suspend fun peekWait(): T {
        while (true) {
            when (queue.size > peekCount) {
                true -> return queue[peekCount++]
                false -> delay(500)
            }
        }
    }

    fun done() {
        (0 .. peekCount).forEach { _ -> queue.removeAt(0) }
    }

    fun untilDone() {
        (0 until peekCount).forEach { _ -> queue.removeAt(0) }
    }

    fun reset() {
        peekCount = 0
    }
}

enum class QueueOrder {
    CONTINUE,
    DONE,
    UNTIL_DONE,
    RESET
}

