package com.example.komaexchange.entities

import io.andrewohara.dynamokt.DynamoKtPartitionKey
import io.andrewohara.dynamokt.DynamoKtSortKey
import software.amazon.awssdk.enhanced.dynamodb.Key
import java.math.BigDecimal

data class Asset(
    @DynamoKtPartitionKey
    val userId: Long, // ユーザID
    val jpyOnHandAmount: BigDecimal, // JPY資産
    val jpyLockedAmount: BigDecimal, // JPYロック資産
    val btcOnHandAmount: BigDecimal, // BTC資産
    val btcLockedAmount: BigDecimal, // BTCロック資産
    val ethOnHandAmount: BigDecimal, // ETH資産
    val ethLockedAmount: BigDecimal, // ETHロック資産
    val updatedAtNs: Long, // 更新日時NS
    val createdAtNs: Long, // 作成日時NS
) {
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

    fun getOnHandAmount(currency: Currency): BigDecimal {
        return when(currency) {
            Currency.JPY -> jpyOnHandAmount
            Currency.BTC -> btcOnHandAmount
            Currency.ETH -> ethOnHandAmount
        }
    }

    fun getLockedAmount(currency: Currency): BigDecimal {
        return when(currency) {
            Currency.JPY -> jpyLockedAmount
            Currency.BTC -> btcLockedAmount
            Currency.ETH -> ethLockedAmount
        }
    }

    fun setOnHandAmount(currency: Currency, amount: BigDecimal): Asset {
        return when(currency) {
            Currency.JPY -> copy(jpyOnHandAmount = amount)
            Currency.BTC -> copy(btcOnHandAmount = amount)
            Currency.ETH -> copy(ethOnHandAmount = amount)
        }
    }

    fun setLockedAmount(currency: Currency, amount: BigDecimal): Asset {
        return when(currency) {
            Currency.JPY -> copy(jpyLockedAmount = amount)
            Currency.BTC -> copy(btcLockedAmount = amount)
            Currency.ETH -> copy(ethLockedAmount = amount)
        }
    }
}
