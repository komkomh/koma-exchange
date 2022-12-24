package com.example.komaexchange

import com.example.komaexchange.entities.ShardMaster
import com.example.komaexchange.entities.ShardStatus
import com.example.komaexchange.repositories.ShardMasterRepository
import com.example.komaexchange.wokrkers.Worker
import io.andrewohara.dynamokt.DataClassTableSchema
import kotlinx.coroutines.*
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient
import java.time.LocalDateTime

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
            .map { shard -> ShardReceiver(streamArn, shard, worker.new()).execute() }
            // map形式にする
            .associateBy { it.shard.shardId() }

        // 保持しているシャード受信に追加する
        shardReceiverMap += newShardReceivers;
    }
}

class ShardReceiver<T : Any>(private val streamArn: String, val shard: Shard, private var worker: Worker<T>) {
    var job: Job? = null
    private val shardMasterRepository = ShardMasterRepository()

    //    private val tableSchema = TableSchema.builder(worker.getEntityClazz()).build()
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
        val shardIteratorRequest = if (shardMaster.shardStatus == ShardStatus.CREATED) {
            GetShardIteratorRequest
                .builder()
                .streamArn(streamArn)
                .shardId(shard.shardId())
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .build()
        } else {
            GetShardIteratorRequest
                .builder()
                .streamArn(streamArn)
                .shardId(shard.shardId())
                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .sequenceNumber(shardMaster.sequenceNumber)
                .build()
        }

        // ShardIteratorを取得する
        val shardIteratorResult = streamsClient.getShardIterator(shardIteratorRequest)

        // Shardから繰り返しレコードを取得する
        var nextShardIterator: String? = shardIteratorResult.shardIterator()
        do {
            val loopStart = System.currentTimeMillis()
            println("${System.currentTimeMillis()} : loop1")
            // レコードを取得する
            val request = GetRecordsRequest.builder().shardIterator(nextShardIterator).build()
            val recordsResult = streamsClient.getRecords(request)
            println("${System.currentTimeMillis()} : loop2")

            // worker処理を実行する
            recordsResult.records().forEach { record ->
                val start = System.currentTimeMillis()
                if (!executeWorker(record)) {
                    if (!executeWorker(record)) {
                        if (!executeWorker(record)) {
                            throw RuntimeException("3回実行してもトランザクション保存できなかった")
                        }
                    }
                }
                println("recordsEnd = ${System.currentTimeMillis() - start}")
            }

            println("${System.currentTimeMillis()} : ${shardMaster.shardId} loop3")
            // 次のイテレータを設定する
            nextShardIterator = recordsResult.nextShardIterator()
            println("${System.currentTimeMillis()} : ${shardMaster.shardId} loop4")

            // 処理レコードがなければ
            if (recordsResult.records().isEmpty()) {
                // 1秒間停止する
                delay(1000)
            }
            if (shardMaster.shardId != "shardId-00000001671779347318-eb9edf8d") {
                delay(100000)
            }
            println("loopEnd = ${System.currentTimeMillis()} : ${System.currentTimeMillis() - loopStart} : ${shardMaster.shardId} size = ${recordsResult.records().size}")

        } while (nextShardIterator != null) // 次のイテレータがなければ終了する

        // シャードが終了したことを永続化する
        shardMasterRepository.save(shardMaster.done())
    }

    // workerを実行する
    private fun executeWorker(record: Record): Boolean {
        val transaction = when (record.eventName()!!) {
            OperationType.INSERT -> {
                val newImage = tableSchema.mapToItem(record.dynamodb().newImage())
                this.worker.insert(newImage)
            }

            OperationType.MODIFY -> {
                val newImage = tableSchema.mapToItem(record.dynamodb().newImage())
                this.worker.modify(newImage)
            }

            OperationType.REMOVE -> {
                val oldImage = tableSchema.mapToItem(record.dynamodb().oldImage())
                this.worker.remove(oldImage)
            }

            OperationType.UNKNOWN_TO_SDK_VERSION -> throw RuntimeException("found UNKNOWN_TO_SDK_VERSION")
        }
        // シーケンスNo(どこまで進んだか)を更新する
        val nextShardMaster = shardMaster.next(record.dynamodb().sequenceNumber())

        println("${System.currentTimeMillis()} : executeWorker3 ")
        // トランザクション保存する(all or nothing)
        if (transaction != null) {
            val transactionResult =
                shardMasterRepository.saveTransaction(nextShardMaster, transaction.transactionRequestBuilder)
            println("${System.currentTimeMillis()} : executeWorker4 ")
            if (!transactionResult) {
                return false
            }
            // 事後処理を行う
            transaction.successFun()
        }
        this.shardMaster = nextShardMaster
        return true
    }
}

enum class ShardReceiveStatus {
    STILL, RUNNING, STOPPED, DONE;

    companion object {
        fun getStatus(job: Job?): ShardReceiveStatus {
            if (job == null) {
                return STILL
            }
            if (job.isActive) {
                return RUNNING
            }
            if (job.isCancelled) {
                return STOPPED
            }
            if (job.isCompleted) {
                return DONE
            }
            throw RuntimeException("ShardExecuteStatus unknown")
        }
    }
}

enum class StreamEvent {
    INSERT, MODIFY, REMOVE
}