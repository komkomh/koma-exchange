package com.example.komaexchange.entities

import io.andrewohara.dynamokt.DynamoKtPartitionKey
import io.andrewohara.dynamokt.DynamoKtSortKey

data class ShardMaster(
    @DynamoKtPartitionKey
    val streamArn: String,

    @DynamoKtSortKey
    val shardId: String,
    val sequenceNumber: String,
    val shardStatus: ShardStatus,
    val lockedNs: Long,
) {
    fun isRunning(currentTimeNs: Long): Boolean {
        return ShardStatus.RUNNING == shardStatus
                && currentTimeNs - lockedNs < 3_000_000
    }

    fun isDone(): Boolean {
        return ShardStatus.DONE == shardStatus
    }

    fun next(nextSequenceNumber: String): ShardMaster {
        return ShardMaster(
            streamArn,
            shardId,
            nextSequenceNumber,
            ShardStatus.RUNNING,
            System.nanoTime()
        )
    }

    fun createDone(): ShardMaster {
        return this.copy(
            shardStatus = ShardStatus.DONE,
            lockedNs = System.nanoTime()
        )
    }
}

enum class ShardStatus {
    CREATED, RUNNING, DONE
}