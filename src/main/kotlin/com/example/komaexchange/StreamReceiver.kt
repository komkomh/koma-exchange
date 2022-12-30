package com.example.komaexchange

import com.example.komaexchange.entities.ShardMaster
import com.example.komaexchange.entities.ShardStatus
import com.example.komaexchange.repositories.ShardMasterRepository
import com.example.komaexchange.wokrkers.TradeWorker
import com.example.komaexchange.wokrkers.Worker
import kotlinx.coroutines.*
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient

val streamsClient: DynamoDbStreamsClient = DynamoDbStreamsClient.builder().build()
private val shardMasterRepository = ShardMasterRepository()

class StreamReceiver (
    private val activeStreams: MutableList<Stream> = mutableListOf(),
    private val runningWorkers: MutableMap<String, TradeWorker> = mutableMapOf()
) {
    fun init() {
        activeStreams.addAll(streamsClient.listStreams().streams())
    }

    fun execute() {

        // TODO 起動していないworkerを削除する

        // シャードリストを取得する
        val activeShards = activeStreams
            .map { DescribeStreamRequest.builder().streamArn(it.streamArn()).build() }
            .map { streamsClient.describeStream(it).streamDescription() }
            .filter { it.streamStatus() == StreamStatus.ENABLING || it.streamStatus() == StreamStatus.ENABLED }
            .flatMap { it.shards().map { shard -> Pair(it.streamArn(), shard) } }

        // アクティブなシャードマスタをDBから取得する
        val shardMasters = activeShards
            .map {
                shardMasterRepository.findOne(it.first, it.second.shardId()) ?: ShardMaster(
                    it.first,
                    it.second.shardId(),
                    "",
                    ShardStatus.CREATED,
                    System.currentTimeMillis()
                )
            }
            .filter { it.shardStatus != ShardStatus.DONE }

        // workerを生成する
        val newWorkers = shardMasters
            // 実行中のworkerは無視する
            .filter { !runningWorkers.containsKey(it.shardId) }
            // 生成する
            .map { shard -> TradeWorker(shard) }
            // map形式にする
            .associateBy { it.shardMaster.shardId }

        // 保持workerに追加する
        runningWorkers.putAll(newWorkers)
    }
}

