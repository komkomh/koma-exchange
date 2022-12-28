package com.example.komaexchange.repositories

import com.example.komaexchange.entities.Asset
import com.example.komaexchange.entities.CurrencyPair
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import software.amazon.awssdk.enhanced.dynamodb.Key
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional

private val dynamoDbClient = DynamoDbEnhancedClient.builder().build()

class AssetRepository {

    private val table: DynamoDbTable<Asset> =
        dynamoDbClient.table(Asset::class.java.simpleName, DataClassTableSchema(Asset::class))

    fun createTable() {
        table.createTable();
    }

    fun save(asset: Asset) {
        table.putItem(asset)
    }

    fun findOne(userId: Long): Asset {
        val key = Key
            .builder()
            .partitionValue(userId)
            .build()
        return table.getItem(key)
    }

    fun list(currencyPair: CurrencyPair): List<Asset> {

        val key = Key
            .builder()
            .partitionValue(currencyPair.name)
            .build()
        return table.query(QueryConditional.keyEqualTo(key)).items().toList()
    }
}
