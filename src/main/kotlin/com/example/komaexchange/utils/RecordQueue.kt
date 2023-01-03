package com.example.komaexchange.utils

import com.example.komaexchange.entities.RecordEntity
import com.example.komaexchange.wokrkers.Record
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class RecordQueue<T : RecordEntity> {
    private val mutex: Mutex = Mutex()
    private val recordList = mutableListOf<Record<T>>()
    private var peekCount = 0
    var lastSequenceNumber: String? = null

    suspend fun offer(t: Record<T>): Record<T> {
        mutex.withLock { recordList.add(t) }
        return t
    }

    suspend fun peek(): Record<T> {
        mutex.withLock {
            return when (recordList.size > peekCount) {
                true -> recordList[peekCount++]
                false -> Record.NONE
            }
        }
    }

    suspend fun peekWait(): Record<T> {
        while (true) {
            when (val t = peek()) {
                is Record.NONE -> delay(500)
                else -> return t
            }
        }
    }

    suspend fun done(): RecordQueue<T> {
        mutex.withLock {
            lastSequenceNumber = when (peekCount < 1) {
                true -> null
                false -> recordList[peekCount - 1].currentSequenceNumber()
            }
            (1..peekCount).forEach { _ -> recordList.removeAt(0) }
            peekCount = 0
        }
        return this
    }

    suspend fun untilDone(): RecordQueue<T> {
        mutex.withLock {
            lastSequenceNumber = when (peekCount < 2) {
                true -> null
                false -> recordList[peekCount - 2].currentSequenceNumber()
            }
            (1 until peekCount).forEach { _ -> recordList.removeAt(0) }
            peekCount = 0
        }
        return this
    }

    suspend fun reset(): RecordQueue<T> {
        mutex.withLock {
            lastSequenceNumber = null
            peekCount = 0
        }
        return this
    }

    suspend fun size(): Int {
        return mutex.withLock { recordList.size }
    }
}
