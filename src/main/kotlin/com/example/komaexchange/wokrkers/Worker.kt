package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.ShardMaster
import com.example.komaexchange.entities.ShardStatus
import com.example.komaexchange.repositories.ShardMasterRepository
import com.example.komaexchange.streamsClient
import com.example.komaexchange.utils.MutexQueue
import io.andrewohara.dynamokt.DataClassTableSchema
import kotlinx.coroutines.*
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest
import software.amazon.awssdk.services.dynamodb.model.OperationType
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType
import kotlin.reflect.KClass

private val shardMasterRepository = ShardMasterRepository()

abstract class Worker<T : Any>(val shardMaster: ShardMaster) {

    val queue: MutexQueue<OperationRecord> = MutexQueue()
    private val tableSchema = DataClassTableSchema(getEntityClazz())
    var receiveJob: Job? = null
    var consumeJob: Job? = null

    fun isRunning(): Boolean {
        return when (consumeJob) {
            null -> false
            else -> consumeJob!!.isActive
        }
    }

    sealed class OperationRecord() {
        data class INSERT<T : Any>(val sequenceNumber: String, val t: T) : OperationRecord()
        data class MODIFY<T : Any>(val sequenceNumber: String, val t: T) : OperationRecord()
        data class REMOVE<T : Any>(val sequenceNumber: String, val t: T) : OperationRecord()
        object FINISHED : OperationRecord()
    }

    abstract fun getEntityClazz(): KClass<T>

    abstract fun execute(record: Record<T>?): QueueOrder

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

//            // queueにrecordを登録する
//            recordsResult.records().map { record ->
//                when (record.eventName()!!) {
//                    OperationType.INSERT -> {
//                        Record(
//                            OperationType.INSERT,
//                            record.dynamodb().sequenceNumber(),
//                            tableSchema.mapToItem(record.dynamodb().newImage()),
//                            false,
//                        )
//                    }
//
//                    OperationType.MODIFY -> {
//                        Record(
//                            OperationType.MODIFY,
//                            record.dynamodb().sequenceNumber(),
//                            tableSchema.mapToItem(record.dynamodb().newImage()),
//                            false,
//                        )
//                    }
//
//                    OperationType.REMOVE -> {
//                        Record(
//                            OperationType.REMOVE,
//                            record.dynamodb().sequenceNumber(),
//                            tableSchema.mapToItem(record.dynamodb().oldImage()),
//                            false,
//                        )
//                    }
//
//                    OperationType.UNKNOWN_TO_SDK_VERSION -> throw RuntimeException("found UNKNOWN_TO_SDK_VERSION")
//                }
//            }.forEach {
//                queue.offer(it)
//            }

            // queueにrecordを登録する
            recordsResult.records().map { record ->
                when (record.eventName()!!) {
                    OperationType.INSERT -> OperationRecord.INSERT(
                        record.dynamodb().sequenceNumber(),
                        tableSchema.mapToItem(record.dynamodb().newImage())
                    )

                    OperationType.MODIFY -> OperationRecord.MODIFY(
                        record.dynamodb().sequenceNumber(),
                        tableSchema.mapToItem(record.dynamodb().newImage())
                    )

                    OperationType.REMOVE -> OperationRecord.REMOVE(
                        record.dynamodb().sequenceNumber(),
                        tableSchema.mapToItem(record.dynamodb().oldImage())
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
                // 1秒間停止する
                delay(1000)
            }

            // 次のイテレータを設定する
            nextShardIterator = recordsResult.nextShardIterator()
        }

        // シャードが終了したことを保存する
        queue.offer()
        shardMasterRepository.save(shardMaster.createDone()) // TODO shardMasterをqueueに渡す必要がある
    }

    suspend fun consumeQueue() {
        var queueOrder = QueueOrder.DONE
        while (true) {
            val record = when (queueOrder) {
                QueueOrder.CONTINUE -> queue.peek()
                QueueOrder.DONE -> {
                    val currentRecord = queue.done().peekWait()
                    when (currentRecord.finished) {
                        true -> {
                            shardMasterRepository.save(shardMaster.createDone())
                            return
                        }

                        false -> currentRecord
                    }
                }

                QueueOrder.RESET -> queue.reset().peekWait()
                QueueOrder.UNTIL_DONE -> queue.untilDone().peekWait()
            }
            queueOrder = execute(record)
        }
    }
}

enum class QueueOrder {
    CONTINUE,
    DONE,
    UNTIL_DONE,
    RESET
}

data class Record<T : Any>(
    val operationType: OperationType,
    val sequenceNumber: String,
    val t: T,
    val finished: Boolean
) {
}



