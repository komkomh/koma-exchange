package com.example.komaexchange.utils

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

// TODO shard終了時の考慮を入れる
class MutexQueue<T : Any> {
    private val mutex: Mutex = Mutex()
    private val queue = mutableListOf<T>()
    private var peekCount = 0

    suspend fun offer(t: T): T {
        mutex.withLock {
            queue.add(t)
        }
        return t
    }

    suspend fun peek(): T? {
        mutex.withLock {
            return when (queue.size > peekCount) {
                true -> queue[peekCount++]
                false -> null
            }
        }
    }

    suspend fun peekWait(): T {
        mutex.withLock {
            while (true) {
                when (queue.size > peekCount) {
                    true -> return queue[peekCount++]
                    false -> delay(500)
                }
            }
        }
    }

    suspend fun done() {
        mutex.withLock {
            (0 until peekCount).forEach { _ -> queue.removeAt(0) }
            peekCount = 0
        }
    }

    suspend fun untilDone() {
        mutex.withLock {
            (0 until peekCount-1).forEach { _ -> queue.removeAt(0) }
            peekCount = 0
        }
    }

    suspend fun reset() {
        mutex.withLock {
            peekCount = 0
        }
    }

    suspend fun size(): Int {
        mutex.withLock {
            return queue.size
        }
    }
}
