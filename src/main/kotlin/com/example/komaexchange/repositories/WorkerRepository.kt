package com.example.komaexchange.repositories

import com.example.komaexchange.entities.*
import com.example.komaexchange.wokrkers.QueueOrder
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable
import software.amazon.awssdk.enhanced.dynamodb.Expression
import software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues
import software.amazon.awssdk.enhanced.dynamodb.model.TransactUpdateItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException


object WorkerRepository {

    fun saveTransaction(
        transaction: Transaction,
        oldShardMaster: ShardMaster,
        newShardMaster: ShardMaster,
    ): TransactionResult {

        val requestBuilder = transaction.builder

//        // Shard：sequenceNumberが更新されていればrollback
//        requestBuilder.addUpdateItem(
//            shardMasterTable, TransactUpdateItemEnhancedRequest.builder(ShardMaster::class.java)
//                .conditionExpression(
//                    Expression.builder()
//                        .expression("#sequenceNumber = :sequenceNumber")
//                        .putExpressionName("#sequenceNumber", "sequenceNumber")
//                        .putExpressionValue(":sequenceNumber", toAttributeValue(oldShardMaster.sequenceNumber))
//                        .build()
//                )
//                .conditionExpression(
//                    Expression.builder()
//                        .expression("#lockedMs = :lockedMs")
//                        .putExpressionName("#lockedMs", "lockedMs")
//                        .putExpressionValue(":lockedMs", toAttributeValue(oldShardMaster.lockedMs))
//                        .build()
//                )
//                .item(newShardMaster)
//                .build()
//        )
        requestBuilder.addPutItem(shardMasterTable, newShardMaster)

        val request = requestBuilder.build()
//        if (request.transactWriteItems().size <= 1) {
//            return TransactionResult.SUCCESS
//        }

        return try {
            dynamoDbClient.transactWriteItems(request)
            TransactionResult.SUCCESS
        } catch (e: TransactionCanceledException) {
            println(e)
            TransactionResult.FAILURE
        }
    }
    data class TransactionData(val items: RecordEntity) {

    }
}

data class Transaction(
    val queueOrder: QueueOrder,
    val builder: TransactWriteItemsEnhancedRequest.Builder = TransactWriteItemsEnhancedRequest.builder(),
    val successFun: () -> Unit = {},
    val failureFun: () -> Unit = {},
) {
}