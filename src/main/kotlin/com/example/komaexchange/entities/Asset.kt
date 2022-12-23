package com.example.komaexchange.entities

import io.andrewohara.dynamokt.DynamoKtPartitionKey
import io.andrewohara.dynamokt.DynamoKtSortKey
import software.amazon.awssdk.enhanced.dynamodb.Key
import java.math.BigDecimal

data class Asset(
    @DynamoKtPartitionKey
    val currency: Currency, // 通貨ペア
    @DynamoKtSortKey
    val userId: Long, // ユーザID
    val onHandAmount: BigDecimal, // 資産
    val lockedAmount: BigDecimal, // ロック資産
    val updatedAtNs: Long, // 更新日時NS
    val createdAtNs: Long, // 作成日時NS
) {
    fun fullKey(): Key {
        return Key.builder().partitionValue(currency.name).sortValue(userId).build()
    }
    fun execution(order: Order): Asset? {
        if (order.amount > onHandAmount) {
            return null
        }

        val newOnHandAmount = onHandAmount - order.remainingAmount
        val newUpdatedAtNs = System.nanoTime()
        return Asset(
            currency,
            userId,
            newOnHandAmount,
            lockedAmount,
            newUpdatedAtNs,
            createdAtNs
        )
    }
}
