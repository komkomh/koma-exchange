package com.example.komaexchange.repositories

import com.example.komaexchange.entities.*
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import software.amazon.awssdk.enhanced.dynamodb.Expression
import software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues
import software.amazon.awssdk.enhanced.dynamodb.model.TransactUpdateItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException

private val dynamoDbClient = DynamoDbEnhancedClient.builder().build()
private val orderTable: DynamoDbTable<Order> = dynamoDbClient.table(
    Order::class.java.simpleName, DataClassTableSchema(Order::class)
)
private val tradeTable: DynamoDbTable<Trade> = dynamoDbClient.table(
    Trade::class.java.simpleName, DataClassTableSchema(Trade::class)
)
private val assetTable: DynamoDbTable<Asset> = dynamoDbClient.table(
    Asset::class.java.simpleName, DataClassTableSchema(Asset::class)
)

private val shardMasterTable: DynamoDbTable<ShardMaster> = dynamoDbClient.table(
    ShardMaster::class.java.simpleName, DataClassTableSchema(ShardMaster::class)
)


class OrderWorkerRepository {

    fun saveTransaction(
        orders: Set<Order>,
        trades: Set<Trade>,
        assets: Set<Pair<Asset, Asset>>,
        shardMaster: ShardMaster,
    ): TransactionResult {
        val requestBuilder = TransactWriteItemsEnhancedRequest.builder()
        orders.forEach { requestBuilder.addPutItem(orderTable, it) }
        trades.forEach { requestBuilder.addPutItem(tradeTable, it) }
        assets.forEach {
            // 資産が変更されていればrollback
            requestBuilder.addUpdateItem(
                assetTable, TransactUpdateItemEnhancedRequest.builder(Asset::class.java)
                    .conditionExpression(
                        Expression.builder()
                            .expression("#updatedAtNs = :updatedAtNs")
                            .putExpressionName("#updatedAtNs", "updatedAtNs")
                            .putExpressionValue(":updatedAtNs", AttributeValues.numberValue(it.first.updatedAtNs))
                            .build()
                    )
                    .item(it.second)
                    .build()
            )
        }
        requestBuilder.addPutItem(shardMasterTable, shardMaster)

        return try {
            dynamoDbClient.transactWriteItems(requestBuilder.build())
            TransactionResult.SUCCESS
        } catch (e: TransactionCanceledException) {
            println(e)
            TransactionResult.FAILURE
        }
    }
}