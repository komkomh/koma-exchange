package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.RecordEntity
import com.example.komaexchange.entities.ShardMaster
import com.example.komaexchange.entities.ShardStatus
import com.example.komaexchange.entities.TransactionResult
import com.example.komaexchange.repositories.ShardMasterRepository
import com.example.komaexchange.repositories.Transaction
import com.example.komaexchange.repositories.WorkerRepository
import com.example.komaexchange.streamsClient
import com.example.komaexchange.utils.RecordQueue
import kotlinx.coroutines.*
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest
import software.amazon.awssdk.services.dynamodb.model.OperationType
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType

abstract class Worker<T : RecordEntity>(var shardMaster: ShardMaster) {

    val queue: RecordQueue<T> = RecordQueue()
    var receiveJob: Job? = null
    var consumeJob: Job? = null

    fun isRunning(): Boolean {
        return when (consumeJob) {
            null -> false
            else -> consumeJob!!.isActive
        }
    }

    abstract fun getTableSchema(): TableSchema<T>

    abstract fun recordInserted(entity: T): Transaction
    abstract fun recordModified(entity: T): Transaction
    abstract fun recordRemoved(entity: T): Transaction
    abstract fun recordNone(): Transaction
    abstract fun recordFinished(): Transaction

    fun start() {
        // 処理中、処理済みなら
        if (shardMaster.isDone() || shardMaster.isRunning(System.currentTimeMillis())) {
            // 何もしない
            return
        }

        receiveJob = CoroutineScope(Dispatchers.Default).launch {
            receiveRecord()
        }
        consumeJob = CoroutineScope(Dispatchers.Default).launch {
            consumeQueue()
        }
    }

    private suspend fun receiveRecord() {
        // ShardIteratorRequestを生成する
        val shardIteratorRequest = when (shardMaster.shardStatus) {
            ShardStatus.CREATED -> {
                GetShardIteratorRequest
                    .builder()
                    .streamArn(shardMaster.streamArn)
                    .shardId(shardMaster.shardId)
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .build()
            }

            ShardStatus.RUNNING -> {
                GetShardIteratorRequest
                    .builder()
                    .streamArn(shardMaster.streamArn)
                    .shardId(shardMaster.shardId)
                    .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    .sequenceNumber(shardMaster.sequenceNumber)
                    .build()
            }

            ShardStatus.DONE -> {
                queue.offer(Record.FINISHED) // シャードは終了している
                return
            }
        }

        // ShardIteratorを取得する
        val shardIteratorResult = streamsClient.getShardIterator(shardIteratorRequest)

        // Shardから繰り返しレコードを取得する
        var nextShardIterator: String? = shardIteratorResult.shardIterator()
        var recordZeroCount = 0
        while (nextShardIterator != null) { // 次のイテレータがなければ終了する
            // レコードを取得する
            val request = GetRecordsRequest.builder().shardIterator(nextShardIterator).build()
            val recordsResult = streamsClient.getRecords(request)

            // queueにrecordを登録する
            recordsResult.records().map { record ->
                when (record.eventName()!!) {
                    OperationType.INSERT -> Record.INSERTED(
                        record.dynamodb().sequenceNumber(),
                        getTableSchema().mapToItem(record.dynamodb().newImage())
                    )

                    OperationType.MODIFY -> Record.MODIFIED(
                        record.dynamodb().sequenceNumber(),
                        getTableSchema().mapToItem(record.dynamodb().newImage())
                    )

                    OperationType.REMOVE -> Record.REMOVED(
                        record.dynamodb().sequenceNumber(),
                        getTableSchema().mapToItem(record.dynamodb().oldImage())
                    )

                    OperationType.UNKNOWN_TO_SDK_VERSION -> throw RuntimeException("found UNKNOWN_TO_SDK_VERSION")
                }
            }.forEach {
                queue.offer(it)
            }

            // 処理レコードがなければ
            when (recordsResult.records().isEmpty()) {
                true -> recordZeroCount++
                false -> recordZeroCount = 0
            }
            if (recordZeroCount >= 3) {
                // 100ms間停止する
                delay(100)
            }

            // 次のイテレータを設定する
            nextShardIterator = recordsResult.nextShardIterator()
        }

        // シャードが終了したことを伝える
        queue.offer(Record.FINISHED)
    }

    suspend fun consumeQueue() {
        var queueOrder = QueueOrder.RESET
        while (true) {
            val record = when (queueOrder) {
                QueueOrder.CONTINUE -> queue.peek()
                QueueOrder.DONE -> queue.done().peekWait()
                QueueOrder.RESET -> queue.reset().peekWait()
                QueueOrder.UNTIL_DONE -> queue.untilDone().peekWait()
                QueueOrder.QUIT -> {
                    ShardMasterRepository.save(shardMaster.createDone())
                    break
                }
            }
            // トランザクションを取得する
            val transaction = when (record) {
                is Record.INSERTED -> {
                    record.t.sequenceNumber = record.sequenceNumber
                    recordInserted(record.t)
                }

                is Record.MODIFIED -> recordModified(record.t)
                is Record.REMOVED -> recordRemoved(record.t)
                is Record.NONE -> recordNone()
                is Record.FINISHED -> recordFinished()
            }
            // 最後のシーケンスNoを取得する
            val sequenceNumber = when (queue.lastSequenceNumber) {
                null -> shardMaster.sequenceNumber
                else -> queue.lastSequenceNumber
            }
            // 新規シャードを生成する
            val newShardMaster = shardMaster.copy(
                sequenceNumber = sequenceNumber,
                lockedMs = System.currentTimeMillis()
            )
            // トランザクションを実行し次のQueue操作を取得する
            queueOrder = when (WorkerRepository.saveTransaction(transaction, shardMaster, newShardMaster)) {
                TransactionResult.SUCCESS -> {
                    this.shardMaster = newShardMaster // シャードを置き換える
                    transaction.queueOrder
                }

                TransactionResult.FAILURE -> QueueOrder.RESET // 失敗すれば無かったことにする
            }
        }
    }
}

enum class QueueOrder { CONTINUE, DONE, UNTIL_DONE, RESET, QUIT }

sealed class Record<out T : RecordEntity>() {
    data class INSERTED<out T : RecordEntity>(val sequenceNumber: String, val t: T) : Record<T>() {
        override fun getSequenceNumber(): String? = t.sequenceNumber
    }

    data class MODIFIED<out T : RecordEntity>(val sequenceNumber: String, val t: T) : Record<T>() {
        override fun getSequenceNumber(): String? = t.sequenceNumber
    }

    data class REMOVED<out T : RecordEntity>(val sequenceNumber: String, val t: T) : Record<T>() {
        override fun getSequenceNumber(): String? = t.sequenceNumber
    }

    object NONE : Record<Nothing>() {
        override fun getSequenceNumber(): String? = null
    }

    object FINISHED : Record<Nothing>() {
        override fun getSequenceNumber(): String? = null
    }

    abstract fun getSequenceNumber(): String?
}
