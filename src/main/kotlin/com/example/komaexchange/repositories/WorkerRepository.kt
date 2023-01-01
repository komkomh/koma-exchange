package com.example.komaexchange.repositories

import com.example.komaexchange.entities.*
import software.amazon.awssdk.enhanced.dynamodb.Expression
import software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues
import software.amazon.awssdk.enhanced.dynamodb.model.TransactUpdateItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException


object WorkerRepository {

    fun saveTransaction(
        orders: Set<Order>,
        trades: Set<Trade>,
        assets: Set<Pair<Asset, Asset>>,
        shardMaster: ShardMaster?,
    ): TransactionResult {
        println("saveTransaction: size = ${orders.size + trades.size + assets.size}")
        val requestBuilder = TransactWriteItemsEnhancedRequest.builder()
        orders.forEach { requestBuilder.addPutItem(orderTable, it) }
        trades.forEach { requestBuilder.addPutItem(tradeTable, it) }
        assets.forEach {
            // 資産が変更されていればrollback
            requestBuilder.addUpdateItem(
                assetTable, TransactUpdateItemEnhancedRequest.builder(Asset::class.java)
                    .conditionExpression(
                        Expression.builder()
                            .expression("#updatedAt = :updatedAt")
                            .putExpressionName("#updatedAt", "updatedAt")
                            .putExpressionValue(":updatedAt", AttributeValues.numberValue(it.first.updatedAt))
                            .build()
                    )
                    .item(it.second)
                    .build()
            )
        }

//        // Shard：sequenceNumberが更新されていればrollback TODO
//        requestBuilder.addUpdateItem(
//            shardMasterTable, TransactUpdateItemEnhancedRequest.builder(ShardMaster::class.java)
//                .conditionExpression(
//                    Expression.builder()
//                        .expression("#sequenceNumber = :sequenceNumber")
//                        .putExpressionName("#sequenceNumber", "sequenceNumber")
//                        .putExpressionValue(
//                            ":sequenceNumber",
//                            AttributeValues.stringValue(shardMaster.first.sequenceNumber)
//                        )
//                        .build()
//                )
//                .item(shardMaster.second)
//                .build()
//        )
        if (shardMaster != null) {
            requestBuilder.addPutItem(shardMasterTable, shardMaster)
        }

        return try {
            dynamoDbClient.transactWriteItems(requestBuilder.build())
            TransactionResult.SUCCESS
        } catch (e: TransactionCanceledException) {
            println(e)
            TransactionResult.FAILURE
        }
    }
    data class TransactionData(val items: RecordEntity) {

    }
}