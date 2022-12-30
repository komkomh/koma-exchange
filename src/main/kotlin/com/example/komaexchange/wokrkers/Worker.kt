package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.ShardMaster
import com.example.komaexchange.repositories.ShardMasterRepository
import io.andrewohara.dynamokt.DataClassTableSchema
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import software.amazon.awssdk.services.dynamodb.model.OperationType
import kotlin.reflect.KClass

private val shardMasterRepository = ShardMasterRepository()

abstract class Worker<T : Any>(val shardMaster: ShardMaster) {

    val mutex: Mutex = Mutex()
    val queue: Queue<Record<T>> = Queue()
    val tableSchema = DataClassTableSchema(getEntityClazz())
    var job: Job? = null

    abstract fun getEntityClazz(): KClass<T>
    abstract fun new(): Worker<T>
    abstract fun execute(record: Record<T>?): QueueOrder

    fun execute() {
        this.job = CoroutineScope(Dispatchers.Default).launch {
            consumeQueue()
        }
    }

    suspend fun consumeQueue() {
        var record: Record<T>? = queue.peekWait()
        while (true) {
            record = when (execute(record)) {
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
        (0..peekCount).forEach { _ -> queue.removeAt(0) }
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

data class Record<T : Any>(val operationType: OperationType, val sequenceNumber: String, val t: T) {
}

