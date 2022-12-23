package com.example.komaexchange.repositories

import com.example.komaexchange.entities.Trade
import io.andrewohara.dynamokt.DataClassTableSchema
import io.andrewohara.dynamokt.createTableWithIndices
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable

private val dynamoDbClient = DynamoDbEnhancedClient.builder().build()

private val tradeTable: DynamoDbTable<Trade> = dynamoDbClient.table(
    Trade::class.java.simpleName, DataClassTableSchema(Trade::class)
)

class TradeRepository {
    fun createTable() {
        tradeTable.createTableWithIndices()
    }
}