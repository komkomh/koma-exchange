package com.example.komaexchange.repositories

import com.example.komaexchange.entities.Asset
import com.example.komaexchange.entities.Order
import com.example.komaexchange.entities.ShardMaster
import com.example.komaexchange.entities.Trade
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable

val dynamoDbClient: DynamoDbEnhancedClient = DynamoDbEnhancedClient.builder().build()

val orderTable: DynamoDbTable<Order> = dynamoDbClient.table(
    Order::class.java.simpleName, DataClassTableSchema(Order::class)
)
val activeIndex = orderTable.index(Order.ActiveIndex)!!

val tradeTable: DynamoDbTable<Trade> = dynamoDbClient.table(
    Trade::class.java.simpleName, DataClassTableSchema(Trade::class)
)

val assetTable: DynamoDbTable<Asset> = dynamoDbClient.table(
    Asset::class.java.simpleName, DataClassTableSchema(Asset::class)
)

val shardMasterTable: DynamoDbTable<ShardMaster> = dynamoDbClient.table(
    ShardMaster::class.java.simpleName, DataClassTableSchema(ShardMaster::class)
)
