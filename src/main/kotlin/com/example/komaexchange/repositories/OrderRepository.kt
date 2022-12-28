package com.example.komaexchange.repositories

import com.example.komaexchange.entities.*
import io.andrewohara.dynamokt.DataClassTableSchema
import io.andrewohara.dynamokt.createTableWithIndices
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import software.amazon.awssdk.enhanced.dynamodb.Expression
import software.amazon.awssdk.enhanced.dynamodb.Key
import software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional
import software.amazon.awssdk.enhanced.dynamodb.model.TransactUpdateItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest

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

private val activeIndex = orderTable.index(Order.ActiveIndex);

class OrderRepository {
    fun createTable() {
        orderTable.createTableWithIndices()
    }

    fun save(order: Order) {
        orderTable.putItem(order)
    }

    fun findOne(currencyPair: CurrencyPair, userId: Long): Order {
        val key = Key.builder()
            .partitionValue(currencyPair.name)
            .sortValue(userId)
            .build()
        return orderTable.getItem(key)
    }

    fun find(currencyPair: CurrencyPair): List<Order> {
        val key = Key.builder()
            .partitionValue(currencyPair.name)
            .build()
        return orderTable.query(QueryConditional.keyEqualTo(key)).items().toList()
    }

    fun findActive(currencyPair: CurrencyPair, active: OrderActive = OrderActive.ACTIVE): List<Order> {
        val key = Key.builder()
            .partitionValue(currencyPair.name)
            .sortValue(active.name)
            .build()
        return activeIndex.query(QueryConditional.keyEqualTo(key)).flatMap { it -> it.items() }
    }

    fun createExecutionRequest(
        orders: List<Order>,
        trades: List<Trade>,
        oldAsset: Asset,
        asset: Asset
    ): TransactWriteItemsEnhancedRequest.Builder {
        val requestBuilder = TransactWriteItemsEnhancedRequest.builder()
        orders.forEach { requestBuilder.addPutItem(orderTable, it) }
        trades.forEach { requestBuilder.addPutItem(tradeTable, it) }

        // 資産が変更されていればrollback
        requestBuilder.addUpdateItem(
            assetTable, TransactUpdateItemEnhancedRequest.builder(Asset::class.java)
                .conditionExpression(
                    Expression
                        .builder()
                        .expression("#updatedAtNs = :updatedAtNs")
                        .putExpressionName("#updatedAtNs", "updatedAtNs")
                        .putExpressionValue(":updatedAtNs", AttributeValues.numberValue(oldAsset.updatedAtNs))
                        .build()
                )
                .item(asset)
                .build()
        )
        return requestBuilder
    }

    fun createCancelRequest(order: Order): TransactWriteItemsEnhancedRequest.Builder {
        return TransactWriteItemsEnhancedRequest.builder().addPutItem(orderTable, order)
    }

//    fun saveExecution(
//        orders: List<Order>,
//        trades: List<Trade>,
//        oldAsset: Asset,
//        asset: Asset
//    ): TransactWriteItemsEnhancedRequest.Builder {
//        val requestBuilder = TransactWriteItemsEnhancedRequest.builder()
//        orders.forEach { requestBuilder.addPutItem(orderTable, it) }
//        trades.forEach { requestBuilder.addPutItem(tradeTable, it) }
//        requestBuilder.addPutItem(assetTable, asset)
//
//        // ロールバック条件を追加
//        val expression = Expression.builder()
//            .expression("#createdAtNs = :createdAtNs")
//            .expressionValues(mapOf(":createdAtNs" to AttributeValues.numberValue(oldAsset.updatedAtNs)))
//            .build();
//        requestBuilder.addConditionCheck(
//            assetTable, ConditionCheck
//                .builder()
//                .key(asset.fullKey())
//                .conditionExpression(expression)
//                .build<Asset>()
//        )
//        return requestBuilder
//    }
//
//    fun saveOrderCancel(order: Order): TransactWriteItemsEnhancedRequest.Builder {
//        return TransactWriteItemsEnhancedRequest.builder().addPutItem(orderTable, order)
//    }
}