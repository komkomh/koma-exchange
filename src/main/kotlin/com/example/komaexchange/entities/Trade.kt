package com.example.komaexchange.entities

import io.andrewohara.dynamokt.DynamoKtPartitionKey
import io.andrewohara.dynamokt.DynamoKtSortKey
import java.math.BigDecimal
import java.time.LocalDateTime

data class Trade(
    @DynamoKtPartitionKey
    val currencyPair: CurrencyPair, // 通貨ペア

    @DynamoKtSortKey
    val tradeId: String,

    val userId: Long, // ユーザID
    val orderSide: OrderSide, // 売買
    val orderType: OrderType, // 注文の方法
    val orderStatus: OrderStatus, // 注文の状態
    val price: BigDecimal, // 価格
    val averagePrice: BigDecimal, // 平均価格
    val amount: BigDecimal, // 数量
    val tradeAction: TradeAction, // メイカーテイカー
    val orderId: Long, // 注文ID

    val targetOrderId: Long, // 約定相手の注文ID
    val targetTradeId: String, // 約定相手の約定ID
    val targetUserId: Long, // 約定相手のユーザID

    val fee: BigDecimal, // 約定手数料
    val updatedAtNs: Long, // 更新日時ns
    val createdAtNs: Long, // 作成日時ns
    val createdAt: String = LocalDateTime.now().toString()
) {
}
