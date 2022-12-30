package com.example.komaexchange

import com.example.komaexchange.entities.ShardMaster
import com.example.komaexchange.entities.ShardStatus
import com.example.komaexchange.repositories.ShardMasterRepository
import com.example.komaexchange.wokrkers.Record
import com.example.komaexchange.wokrkers.Worker
import io.andrewohara.dynamokt.DataClassTableSchema
import kotlinx.coroutines.*
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

val streamsClient: DynamoDbStreamsClient = DynamoDbStreamsClient.builder().build()

class StreamReceiver<T : Any>(
    private val worker: Worker<T>,
    private var shardReceiverMap: Map<String, ShardReceiver<T>> = mapOf()
) {
    fun execute() {
        // ストリームを取得する
        val listStreamRequest = ListStreamsRequest
            .builder()
            .tableName(worker.getEntityClazz().simpleName)
            .build()
        val listStreamResponse = streamsClient.listStreams(listStreamRequest)
        val streamArn = listStreamResponse.streams().first().streamArn()

        // シャードリストを取得する
        val describeStreamRequest = DescribeStreamRequest.builder().streamArn(streamArn).build()
        val describeStreamResult = streamsClient.describeStream(describeStreamRequest)
        val shards = describeStreamResult.streamDescription().shards()

        // シャード受信を生成する
        val newShardReceivers = shards
            // 過去分は無視する
            .filter { !shardReceiverMap.containsKey(it.shardId()) }
            // 生成する
            .map { shard -> ShardReceiver(streamArn, shard, worker.new()) }
            // map形式にする
            .associateBy { it.shard.shardId() }

        // 保持しているシャード受信に追加する
        shardReceiverMap += newShardReceivers;
    }
}

class ShardReceiver<T : Any>(private val streamArn: String, val shard: Shard, private var worker: Worker<T>) {
    var job: Job? = null
    private val shardMasterRepository = ShardMasterRepository()
    private val tableSchema = DataClassTableSchema(worker.getEntityClazz())

    // シャードマスタを取得、無ければ生成する
    private var shardMaster = shardMasterRepository.findOne(streamArn, shard.shardId())
        ?: shardMasterRepository.save(
            ShardMaster(
                streamArn = streamArn,
                shardId = shard.shardId(),
                sequenceNumber = "",
                shardStatus = ShardStatus.CREATED,
                lockedNs = System.nanoTime()
            )
        )

    fun execute(): ShardReceiver<T> {
        // 処理中、処理済みなら
        if (shardMaster.isDone() || shardMaster.isRunning(System.nanoTime())) {
            // 何もしない
            return this
        }

        job = CoroutineScope(Dispatchers.Default).launch {
            receiveRecord()
        }
        return this
    }

    private suspend fun receiveRecord() {
        // ShardIteratorRequestを生成する
        val shardIteratorRequest = when (shardMaster.shardStatus) {
            ShardStatus.CREATED -> {
                GetShardIteratorRequest
                    .builder()
                    .streamArn(streamArn)
                    .shardId(shard.shardId())
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .build()
            }

            ShardStatus.RUNNING, ShardStatus.DONE -> {
                GetShardIteratorRequest
                    .builder()
                    .streamArn(streamArn)
                    .shardId(shard.shardId())
                    .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    .sequenceNumber(shardMaster.sequenceNumber)
                    .build()
            }
        }

        // ShardIteratorを取得する
        val shardIteratorResult = streamsClient.getShardIterator(shardIteratorRequest)

        // Shardから繰り返しレコードを取得する
        var nextShardIterator: String? = shardIteratorResult.shardIterator()
        var recordZeroCount = 0
        while (nextShardIterator != null) { // 次のイテレータがなければ終了する
            val loopStart = System.currentTimeMillis()
            // レコードを取得する
            val request = GetRecordsRequest.builder().shardIterator(nextShardIterator).build()
            val recordsResult = streamsClient.getRecords(request)

            // queueにrecordを登録する
            recordsResult.records().map { record ->
                when (record.eventName()!!) {
                    OperationType.INSERT -> {
                        Record(
                            OperationType.INSERT,
                            record.dynamodb().sequenceNumber(),
                            tableSchema.mapToItem(record.dynamodb().newImage()),
                        )
                    }

                    OperationType.MODIFY -> {
                        Record(
                            OperationType.MODIFY,
                            record.dynamodb().sequenceNumber(),
                            tableSchema.mapToItem(record.dynamodb().newImage()),
                        )
                    }

                    OperationType.REMOVE -> {
                        Record(
                            OperationType.REMOVE,
                            record.dynamodb().sequenceNumber(),
                            tableSchema.mapToItem(record.dynamodb().oldImage()),
                        )
                    }

                    OperationType.UNKNOWN_TO_SDK_VERSION -> throw RuntimeException("found UNKNOWN_TO_SDK_VERSION")
                }
            }.forEach {
                worker.queue.offer(it)
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
        shardMasterRepository.save(shardMaster.createDone()) // TODO shardMasterをqueueに渡す必要がある
    }
}