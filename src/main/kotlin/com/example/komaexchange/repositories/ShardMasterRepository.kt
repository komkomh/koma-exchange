package com.example.komaexchange.repositories

import com.example.komaexchange.entities.ShardMaster
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import software.amazon.awssdk.enhanced.dynamodb.Key
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException

private val dynamoDbClient = DynamoDbEnhancedClient.builder().build()

private val ShardMasterTable: DynamoDbTable<ShardMaster> = dynamoDbClient.table(
    ShardMaster::class.java.simpleName, DataClassTableSchema(ShardMaster::class)
)

class ShardMasterRepository {

    fun createTable() {
        ShardMasterTable.createTable();
    }

    fun save(shardMaster: ShardMaster): ShardMaster {
        ShardMasterTable.putItem(shardMaster)
        return shardMaster
    }

    fun saveTransaction(shardMaster: ShardMaster, requestBuilder: TransactWriteItemsEnhancedRequest.Builder?): Boolean {
        return try {
            val builder = requestBuilder ?: TransactWriteItemsEnhancedRequest.builder()
            val transactionRequest = builder.addPutItem(ShardMasterTable, shardMaster).build()
            dynamoDbClient.transactWriteItems(transactionRequest)
            true
        } catch (e: TransactionCanceledException) {
            println(e)
            false
        }
    }

    fun findOne(streamArn: String, shardId: String): ShardMaster? {
        val key = Key.builder()
            .partitionValue(streamArn)
            .sortValue(shardId)
            .build()
        return ShardMasterTable.getItem(key)
    }

    fun list(streamArn: String): List<ShardMaster> {
        val key = Key.builder()
            .partitionValue(streamArn)
            .build()
        return ShardMasterTable.query(QueryConditional.keyEqualTo(key)).items().toList()
    }
}