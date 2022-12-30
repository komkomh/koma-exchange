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

fun main() {
    val assetRepository = AssetRepository()
    val oldAsset = assetRepository.findOne(10L)
    val newAsset = oldAsset.copy(jpyOnHandAmount = BigDecimal(100002), updatedAt = 4L)
    val asset2 = Asset(
        11L, // ユーザID
        BigDecimal(13), // JPY資産
        BigDecimal(13), // BTC資産
        BigDecimal(13), // ETH資産
        4L, // 更新日時NS
        0L, // 作成日時NS
    )

    val transactionRequest = TransactWriteItemsEnhancedRequest
        .builder()
        .addUpdateItem(
            assetTable, TransactUpdateItemEnhancedRequest.builder(Asset::class.java)
                .conditionExpression(
                    Expression
                        .builder()
                        .expression("#updatedAt = :updatedAt")
                        .putExpressionName("#updatedAt", "updatedAt")
                        .putExpressionValue(":updatedAt", AttributeValues.numberValue(oldAsset.updatedAt))
                        .build()
                )
                .item(newAsset)
                .build()
        )
        .addPutItem(assetTable, asset2)
        .build()
    dynamoDbClient.transactWriteItems(transactionRequest)
}
