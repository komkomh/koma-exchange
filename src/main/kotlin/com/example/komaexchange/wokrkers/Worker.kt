package com.example.komaexchange.wokrkers

import kotlinx.coroutines.delay
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.reflect.KClass

abstract class Worker<T : Any> {

    val queue: Queue<T> = Queue()

    abstract fun getEntityClazz(): KClass<T>;
    abstract fun new(): Worker<T>;

    abstract fun insert(t: T): Transaction?
    abstract fun modify(t: T): Transaction?
    abstract fun remove(t: T): Transaction?
}

class Queue<T : Any> {
    val queue: ConcurrentLinkedQueue<T> = ConcurrentLinkedQueue()
    var peekCount = 0
    var waitingTime = 0L
    suspend fun peek(): T {
        val t = queue.peek()

        if (t == null) {
            delay(waitingTime)
            waitingTime = calcNextWaitingTime()
        }
        return t
    }

    fun calcNextWaitingTime(): Long {
        if (waitingTime == 0L) {
            return 1L
        }

        val newWaitingTime = waitingTime * 2
        return when (newWaitingTime > 500) {
            true -> 500
            false -> newWaitingTime
        }
    }

    fun commit() {
        (0 until peekCount).forEach { _ -> queue.remove() }
    }

    fun rollback() {
        peekCount = 0
        waitingTime = 0L
    }
}

data class Transaction(
    val transactionRequestBuilder: TransactWriteItemsEnhancedRequest.Builder,
    val successFun: () -> Unit
) {
}