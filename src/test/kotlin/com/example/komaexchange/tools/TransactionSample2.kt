package com.example.komaexchange.tools

import com.example.komaexchange.entities.*
import com.example.komaexchange.repositories.AssetRepository
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import software.amazon.awssdk.enhanced.dynamodb.Expression
import software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues
import software.amazon.awssdk.enhanced.dynamodb.model.TransactUpdateItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest
import java.math.BigDecimal

private val dynamoDbClient = DynamoDbEnhancedClient.builder().build()
private val assetTable: DynamoDbTable<Asset> = dynamoDbClient.table(
    Asset::class.java.simpleName, DataClassTableSchema(Asset::class)
)
private val orderTable: DynamoDbTable<Order> = dynamoDbClient.table(
    Order::class.java.simpleName, DataClassTableSchema(Order::class)
)

fun main() {
    val assetRepository = AssetRepository()
    val oldAsset = assetRepository.findOne(Currency.JPY, 10L)
    val newAsset = oldAsset.copy(onHandAmount = BigDecimal(100002), updatedAtNs = 4L)
    val asset2 = Asset(
        Currency.JPY, // 通貨ペア
        11L, // ユーザID
        BigDecimal(13), // 資産
        BigDecimal(0), // ロック資産
        4L, // 更新日時NS
        0L, // 作成日時NS
    )

    val transactionRequestBuilder = TransactWriteItemsEnhancedRequest
        .builder()
        .addUpdateItem(
            assetTable, TransactUpdateItemEnhancedRequest.builder(Asset::class.java)
                .conditionExpression(
                    Expression
                        .builder()
                        .expression("#updatedAtNs = :updatedAtNs")
                        .putExpressionName("#updatedAtNs", "updatedAtNs")
                        .putExpressionValue(":updatedAtNs", AttributeValues.numberValue(oldAsset.updatedAtNs))
                        .build()
                )
                .item(newAsset)
                .build()
        )
        .addPutItem(assetTable, asset2);
    val orders = (1L..98L).map { createOrder(it) }
    orders.forEach { transactionRequestBuilder.addPutItem(orderTable, it) }
    dynamoDbClient.transactWriteItems(transactionRequestBuilder.build())
}

fun createOrder(orderId: Long): Order {
    return Order(
        CurrencyPair.BTC_JPY, // 通貨ペア
        orderId, // 注文ID
        OrderActive.ACTIVE, // 完了しているか
        10L, // ユーザID
        OrderSide.SELL, // 売買
        OrderType.LIMIT, // 注文の方法
        OrderStatus.UNFILLED, // 注文の状態
        BigDecimal(999), // 価格
        BigDecimal(10), // 平均価格
        BigDecimal(1), // 数量
        BigDecimal(1), // 残数量
        TradeAction.TAKER, // メイカーテイカー
        "", // 処理ID
        System.nanoTime(), // 更新日時ns
        System.nanoTime(), // 作成日時ns
        BigDecimal.ZERO, // 変更数量
        ActionRequest.ORDER, // 操作要求
        ActionResult.YET, // 操作結果
    )
}