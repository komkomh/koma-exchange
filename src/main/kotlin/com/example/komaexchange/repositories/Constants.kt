package com.example.komaexchange.repositories

import com.example.komaexchange.entities.*
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import kotlin.reflect.KClass

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

val recordTable: DynamoDbTable<RecordEntity> = dynamoDbClient.table(
    ShardMaster::class.java.simpleName, DataClassTableSchema(RecordEntity::class)
)

val tableMap: Map<KClass<out Any>, DynamoDbTable<out Any>> = mapOf(
    Order::class to orderTable,
    Trade::class to tradeTable,
)
