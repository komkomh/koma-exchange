package com.example.komaexchange.repositories

import com.example.komaexchange.entities.ShardMaster
import software.amazon.awssdk.enhanced.dynamodb.Key
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional

object ShardMasterRepository {

    fun createTable() {
        shardMasterTable.createTable();
    }

    fun save(shardMaster: ShardMaster): ShardMaster {
        shardMasterTable.putItem(shardMaster)
        return shardMaster
    }

    fun findOne(streamArn: String, shardId: String): ShardMaster? {
        val key = Key.builder()
            .partitionValue(streamArn)
            .sortValue(shardId)
            .build()
        return shardMasterTable.getItem(key)
    }

    fun list(streamArn: String): List<ShardMaster> {
        val key = Key.builder()
            .partitionValue(streamArn)
            .build()
        return shardMasterTable.query(QueryConditional.keyEqualTo(key)).items().toList()
    }
}