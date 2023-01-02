package com.example.komaexchange.repositories

import com.example.komaexchange.entities.Asset
import com.example.komaexchange.entities.CurrencyPair
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import software.amazon.awssdk.enhanced.dynamodb.Key
import software.amazon.awssdk.enhanced.dynamodb.model.BatchGetItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional
import software.amazon.awssdk.enhanced.dynamodb.model.ReadBatch

object AssetRepository {

    fun createTable() {
        assetTable.createTable();
    }

    fun save(asset: Asset) {
        assetTable.putItem(asset)
    }

    fun findOne(userId: Long): Asset {
        val key = Key
            .builder()
            .partitionValue(userId)
            .build()
        return assetTable.getItem(key)
    }

    fun find(userIds: Set<Long>): Set<Asset> {
        val readBatches = userIds
            .map { Key.builder().partitionValue(it).build() }
            .map { GetItemEnhancedRequest.builder().key(it).build() }
            .map { ReadBatch.builder(Asset::class.java).addGetItem(it).build() }

        val request = BatchGetItemEnhancedRequest.builder().readBatches(readBatches).build()
        return dynamoDbClient.batchGetItem(request).flatMap { it.resultsForTable(assetTable) }.toSet()

    }

    fun list(currencyPair: CurrencyPair): List<Asset> {

        val key = Key
            .builder()
            .partitionValue(currencyPair.name)
            .build()
        return assetTable.query(QueryConditional.keyEqualTo(key)).items().toList()
    }
}
