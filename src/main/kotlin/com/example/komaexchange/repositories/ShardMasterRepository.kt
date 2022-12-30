package com.example.komaexchange.repositories

import com.example.komaexchange.entities.ShardMaster
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import software.amazon.awssdk.enhanced.dynamodb.Key
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional

private val dynamoDbClient = DynamoDbEnhancedClient.builder().build()

private val shardMasterTable: DynamoDbTable<ShardMaster> = dynamoDbClient.table(
    ShardMaster::class.java.simpleName, DataClassTableSchema(ShardMaster::class)
)

class ShardMasterRepository {

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