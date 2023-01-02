package com.example.komaexchange.entities

import io.andrewohara.dynamokt.DynamoKtPartitionKey
import io.andrewohara.dynamokt.DynamoKtSecondaryPartitionKey
import io.andrewohara.dynamokt.DynamoKtSecondarySortKey
import io.andrewohara.dynamokt.DynamoKtSortKey
import java.math.BigDecimal
import java.util.*

data class Order(
    @DynamoKtPartitionKey
    @DynamoKtSecondaryPartitionKey(indexNames = [ActiveIndex])
    val currencyPair: CurrencyPair, // 通貨ペア

    @DynamoKtSortKey
    val orderId: Long, // 注文ID

    @DynamoKtSecondarySortKey(indexNames = [ActiveIndex])
    val orderActive: OrderActive, // 完了しているか
    val userId: Long, // ユーザID
    val orderSide: OrderSide, // 売買
    val orderType: OrderType, // 注文の方法
    val orderStatus: OrderStatus, // 注文の状態
    val price: BigDecimal?, // 価格
    val averagePrice: BigDecimal, // 平均価格
    val amount: BigDecimal, // 数量
    val remainingAmount: BigDecimal, // 残数量
    val tradeAction: TradeAction, // メイカーテイカー
    override var sequenceNumber: String?, // 処理ID
    val updatedAt: Long, // 更新日時ms
    val createdAt: Long, // 作成日時ms
    val changeAmount: BigDecimal, // 変更数量
    val actionRequest: ActionRequest, // 操作要求
    val actionResult: ActionResult, // 操作結果
) : RecordEntity(sequenceNumber), Comparable<Order> {
    companion object {
        const val ActiveIndex: String = "activeIndex"
    }

    // 0は同じと見なされsetに入らないので必ず1, -1を返却する必要がある
    override fun compareTo(o: Order): Int {
        val priceCompare = when (orderSide) {
            OrderSide.BUY -> o.price!!.compareTo(price)
            OrderSide.SELL -> price!!.compareTo(o.price)
        }
        return when (priceCompare) {
            0 -> this.orderId.compareTo(o.orderId)
            else -> priceCompare
        }
        // TODO created_sequence_numberにする必要がある
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Order) return false
        if (currencyPair != other.currencyPair) return false
        if (orderId != other.orderId) return false
        return true
    }

    override fun hashCode(): Int {
        return 31 * currencyPair.hashCode() + orderId.hashCode()
    }

    fun toMakerOrder(): Order {
        return this.copy(
            orderType = OrderType.MARKET,
            updatedAt = System.currentTimeMillis(),
            tradeAction = TradeAction.MAKER,
            actionRequest = ActionRequest.NONE,
            actionResult = ActionResult.ORDER_SUCCEEDED,
        )
    }

    fun createTradedOrder(targetAmount: BigDecimal): Order {
        val newRemainingAmount = remainingAmount - targetAmount
        val orderStatus = OrderStatus.filled(newRemainingAmount)
        return this.copy(
            orderActive = orderStatus.orderActive,
            orderStatus = orderStatus,
            updatedAt = System.currentTimeMillis(),
            actionRequest = ActionRequest.NONE,
            actionResult = ActionResult.ORDER_SUCCEEDED,
            remainingAmount = newRemainingAmount
        )
    }

    fun createTrade(
        targetAmount: BigDecimal,
        tradePrice: BigDecimal,
        oppositeOrder: Order,
        oppositeTradeId: String
    ): Trade {
        return Trade(
            currencyPair, // 通貨ペア
            UUID.randomUUID().toString(), // 約定ID
            userId, // ユーザID
            orderSide, // 売買
            orderType, // 注文の方法
            orderStatus, // 注文の状態
            tradePrice, // 価格
            averagePrice, // 平均価格 TODO
            targetAmount, // 数量
            tradeAction, // メイカーテイカー
            orderId, // 注文ID
            oppositeOrder.orderId, // 約定相手の注文ID
            oppositeTradeId, // 約定相手の約定ID
            oppositeOrder.userId, // 約定相手のユーザID
            BigDecimal.ZERO, // 約定手数料 TODO
            System.currentTimeMillis(), // 更新日時ms
            System.currentTimeMillis(), // 作成日時ms
        )
    }

    fun createCanceledOrder(): Order {
        val orderStatus = OrderStatus.cancel(amount, remainingAmount)
        return this.copy(
            orderActive = orderStatus.orderActive,
            orderStatus = orderStatus,
            updatedAt = System.currentTimeMillis(),
            actionRequest = ActionRequest.NONE,
            actionResult = ActionResult.ORDER_FAILED,
        )
    }
}

enum class OrderActive {
    ACTIVE, IN_ACTIVE
}

enum class OrderSide {
    BUY {
        override fun isCross(takerPrice: BigDecimal, makerPrice: BigDecimal): Boolean {
            return takerPrice >= makerPrice;
        }
    },
    SELL {
        override fun isCross(takerPrice: BigDecimal, makerPrice: BigDecimal): Boolean {
            return takerPrice <= makerPrice;
        }
    };

    abstract fun isCross(takerPrice: BigDecimal, makerPrice: BigDecimal): Boolean;
}

enum class OrderStatus(val orderActive: OrderActive) {
    UNFILLED(OrderActive.ACTIVE), // 未約定
    PARTIALLY_FILLED(OrderActive.ACTIVE), // 部分約定
    FULLY_FILLED(OrderActive.IN_ACTIVE), // 約定済
    CANCELED_UNFILLED(OrderActive.IN_ACTIVE), // 未約定キャンセル
    CANCELED_PARTIALLY_FILLED(OrderActive.IN_ACTIVE); // 部分約定キャンセル

    companion object {
        fun filled(remainingAmount: BigDecimal): OrderStatus {
            return when {
                remainingAmount > BigDecimal.ZERO -> PARTIALLY_FILLED
                else -> FULLY_FILLED
            }
        }

        fun cancel(amount: BigDecimal, remainingAmount: BigDecimal): OrderStatus {
            return when (amount) {
                remainingAmount -> CANCELED_UNFILLED
                else -> CANCELED_PARTIALLY_FILLED
            }
        }
    }
}

enum class OrderType {
    MARKET, // 成行
    LIMIT, // 指値
    POST_ONLY // PostOnly
}

enum class TradeAction {
    MAKER, // メイカー
    TAKER, // テイカー
}

enum class ActionRequest {
    ORDER, // 注文
    ORDER_CHANGE, // 注文変更
    ORDER_CANCEL, // キャンセル
    NONE, // なし
}

enum class ActionResult {
    ORDER_SUCCEEDED, // 注文成功
    ORDER_FAILED, // 注文失敗
    ORDER_CHANGE_SUCCEEDED, // 変更成功
    ORDER_CHANGE_FAILED, // 変更失敗
    ORDER_CANCEL_SUCCEEDED, // キャンセル成功
    ORDER_CANCEL_FAILED, // キャンセル失敗
    YET, // なし
}