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
    val lockedMs: Long,
) {
    fun isRunning(currentTimeMs: Long): Boolean {
        return ShardStatus.RUNNING == shardStatus
                && currentTimeMs - lockedMs < 3_000
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
            System.currentTimeMillis()
        )
    }

    fun createDone(): ShardMaster {
        return this.copy(
            shardStatus = ShardStatus.DONE,
            lockedMs = System.currentTimeMillis()
        )
    }
}

enum class ShardStatus {
    CREATED, RUNNING, DONE
}