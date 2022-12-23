//package com.example.komaexchange.tools
//
//import com.example.helloexchangekotlin.entities.Asset
//import com.example.helloexchangekotlin.entities.Currency
//import com.example.helloexchangekotlin.repositories.AssetRepository
//import io.andrewohara.dynamokt.DataClassTableSchema
//import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
//import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
//import software.amazon.awssdk.enhanced.dynamodb.Expression
//import software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues
//import software.amazon.awssdk.enhanced.dynamodb.model.ConditionCheck
//import software.amazon.awssdk.enhanced.dynamodb.model.TransactUpdateItemEnhancedRequest
//import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest
//import software.amazon.awssdk.enhanced.dynamodb.update.SetAction
//import software.amazon.awssdk.enhanced.dynamodb.update.UpdateExpression
//import java.math.BigDecimal
//
//private val dynamoDbClient = DynamoDbEnhancedClient.builder().build()
//private val assetTable: DynamoDbTable<Asset> = dynamoDbClient.table(
//    Asset::class.java.simpleName, DataClassTableSchema(Asset::class)
//)
//
//fun main() {
//    val assetRepository = AssetRepository()
//    val oldAsset = assetRepository.findOne(Currency.JPY, 10L)
//    val newAsset = oldAsset.copy(onHandAmount = BigDecimal(100002), updatedAtNs = 4L)
//    val asset2 = Asset(
//        Currency.JPY, // 通貨ペア
//        11L, // ユーザID
//        BigDecimal(13), // 資産
//        BigDecimal(0), // ロック資産
//        4L, // 更新日時NS
//        0L, // 作成日時NS
//    )
//
//    val transactionRequest = TransactWriteItemsEnhancedRequest
//        .builder()
//        .addUpdateItem(
//            assetTable, TransactUpdateItemEnhancedRequest.builder(Asset::class.java)
//                .conditionExpression(
//                    Expression
//                        .builder()
//                        .expression("#updatedAtNs = :updatedAtNs")
//                        .putExpressionName("#updatedAtNs", "updatedAtNs")
//                        .putExpressionValue(":updatedAtNs", AttributeValues.numberValue(oldAsset.updatedAtNs))
//                        .build()
//                )
//                .item(newAsset)
//                .build()
//        )
//        .addPutItem(assetTable, asset2)
//        .build()
//    dynamoDbClient.transactWriteItems(transactionRequest)
//    println("kiteru")
//
//}