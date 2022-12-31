package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.ShardMaster
import com.example.komaexchange.entities.ShardStatus
import com.example.komaexchange.repositories.ShardMasterRepository
import com.example.komaexchange.streamsClient
import com.example.komaexchange.utils.RecordQueue
import io.andrewohara.dynamokt.DataClassTableSchema
import kotlinx.coroutines.*
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest
import software.amazon.awssdk.services.dynamodb.model.OperationType
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType
import kotlin.reflect.KClass

abstract class Worker<T : Any>(val shardMaster: ShardMaster) {

    val queue: RecordQueue<T> = RecordQueue()
    private val tableSchema = DataClassTableSchema(getEntityClazz())
    var receiveJob: Job? = null
    var consumeJob: Job? = null

    fun isRunning(): Boolean {
        return when (consumeJob) {
            null -> false
            else -> consumeJob!!.isActive
        }
    }

    abstract fun getEntityClazz(): KClass<T>

    abstract fun execute(record: Record<T>): QueueOrder

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

            // queueにrecordを登録する
            recordsResult.records().map { record ->
                when (record.eventName()!!) {
                    OperationType.INSERT -> Record.INSERTED(
                        record.dynamodb().sequenceNumber(),
                        tableSchema.mapToItem(record.dynamodb().newImage())
                    )

                    OperationType.MODIFY -> Record.MODIFIED(
                        record.dynamodb().sequenceNumber(),
                        tableSchema.mapToItem(record.dynamodb().newImage())
                    )

                    OperationType.REMOVE -> Record.REMOVED(
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

        // シャードが終了したことを伝える
        queue.offer(Record.FINISHED)
    }

    suspend fun consumeQueue() {
        var queueOrder = QueueOrder.DONE
        while (true) {
            val record = when (queueOrder) {
                QueueOrder.CONTINUE -> queue.peek()
                QueueOrder.DONE -> queue.done().peekWait()
                QueueOrder.RESET -> queue.reset().peekWait()
                QueueOrder.UNTIL_DONE -> queue.untilDone().peekWait()
                QueueOrder.QUIT -> break
            }
            queueOrder = execute(record)
        }
    }
}

enum class QueueOrder {
    CONTINUE,
    DONE,
    UNTIL_DONE,
    RESET,
    QUIT
}

sealed class Record<out T: Any>() {
    data class INSERTED<out T: Any>(val sequenceNumber: String, val t: T) : Record<T>()
    data class MODIFIED<out T: Any>(val sequenceNumber: String, val t: T) : Record<T>()
    data class REMOVED<out T: Any>(val sequenceNumber: String, val t: T) : Record<T>()
    object NONE : Record<Nothing>()
    object FINISHED : Record<Nothing>()
}



