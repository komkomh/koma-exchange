package com.example.komaexchange.entities

import io.andrewohara.dynamokt.DynamoKtPartitionKey
import io.andrewohara.dynamokt.DynamoKtSortKey
import software.amazon.awssdk.enhanced.dynamodb.Key
import java.math.BigDecimal

data class Asset(
    @DynamoKtPartitionKey
    val userId: Long, // ユーザID
    val jpyOnHandAmount: BigDecimal, // JPY資産
//    val jpyLockedAmount: BigDecimal, // JPYロック資産
    val btcOnHandAmount: BigDecimal, // BTC資産
//    val btcLockedAmount: BigDecimal, // BTCロック資産
    val ethOnHandAmount: BigDecimal, // ETH資産
//    val ethLockedAmount: BigDecimal, // ETHロック資産
    val updatedAt: Long, // 更新日時
    val createdAt: Long, // 作成日時
) {
    fun execution(order: Order): Asset? {
        // TODO 資産更新
        return copy(
            jpyOnHandAmount = jpyOnHandAmount + BigDecimal(1.0),
            btcOnHandAmount = btcOnHandAmount + BigDecimal(1.0),
            updatedAt = System.currentTimeMillis(),
            createdAt = System.currentTimeMillis(),
        )
    }

    fun getOnHandAmount(currency: Currency): BigDecimal {
        return when(currency) {
            Currency.JPY -> jpyOnHandAmount
            Currency.BTC -> btcOnHandAmount
            Currency.ETH -> ethOnHandAmount
        }
    }

//    fun getLockedAmount(currency: Currency): BigDecimal {
//        return when(currency) {
//            Currency.JPY -> jpyLockedAmount
//            Currency.BTC -> btcLockedAmount
//            Currency.ETH -> ethLockedAmount
//        }
//    }
//
//    fun setOnHandAmount(currency: Currency, amount: BigDecimal): Asset {
//        return when(currency) {
//            Currency.JPY -> copy(jpyOnHandAmount = amount)
//            Currency.BTC -> copy(btcOnHandAmount = amount)
//            Currency.ETH -> copy(ethOnHandAmount = amount)
//        }
//    }
//
//    fun setLockedAmount(currency: Currency, amount: BigDecimal): Asset {
//        return when(currency) {
//            Currency.JPY -> copy(jpyLockedAmount = amount)
//            Currency.BTC -> copy(btcLockedAmount = amount)
//            Currency.ETH -> copy(ethLockedAmount = amount)
//        }
//    }
}
