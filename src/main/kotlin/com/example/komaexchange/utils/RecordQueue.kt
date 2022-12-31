package com.example.komaexchange.utils

import com.example.komaexchange.wokrkers.Record
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class RecordQueue<T : Any> {
    private val mutex: Mutex = Mutex()
    private val queue = mutableListOf<Record<T>>()
    private var peekCount = 0

    suspend fun offer(t: Record<T>): Record<T> {
        mutex.withLock { queue.add(t) }
        return t
    }

    suspend fun peek(): Record<T> {
        mutex.withLock {
            return when (queue.size > peekCount) {
                true -> queue[peekCount++]
                false -> Record.NONE
            }
        }
    }

    suspend fun peekWait(): Record<T> {
        while (true) {
            when(val t = peek()) {
                is Record.NONE -> delay(500)
                else -> return t
            }
        }
    }

    suspend fun done(): RecordQueue<T> {
        mutex.withLock {
            (0 until peekCount).forEach { _ -> queue.removeAt(0) }
            peekCount = 0
        }
        return this
    }

    suspend fun untilDone(): RecordQueue<T> {
        mutex.withLock {
            (0 until peekCount-1).forEach { _ -> queue.removeAt(0) }
            peekCount = 0
        }
        return this
    }

    suspend fun reset(): RecordQueue<T> {
        mutex.withLock { peekCount = 0 }
        return this
    }

    suspend fun size(): Int {
        return mutex.withLock { queue.size }
    }
}
