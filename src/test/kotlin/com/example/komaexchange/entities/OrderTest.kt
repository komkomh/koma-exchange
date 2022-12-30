package com.example.komaexchange.entities

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class OrderTest {

    @Test
    @DisplayName("買のソート順")
    fun buySortTest() {
        val order1 = createTestOrder(1L, OrderSide.BUY, BigDecimal(10), 1L) // 4 価格
        val order2 = createTestOrder(2L, OrderSide.BUY, BigDecimal(20), 2L) // 2 価格、時刻
        val order3 = createTestOrder(3L, OrderSide.BUY, BigDecimal(20), 3L) // 3 価格、時刻
        val order4 = createTestOrder(4L, OrderSide.BUY, BigDecimal(30), 4L) // 1 価格
        val buyOrders = sortedSetOf<Order>(order1, order2, order3, order4)
        Assertions.assertThat(buyOrders.toList())
            .`as`("価格、作成時刻による順序")
            .extracting<Long> { it.orderId }
            .containsExactly(4L, 2L, 3L, 1L)
    }

    @Test
    @DisplayName("売のソート順")
    fun sellSortTest() {
        val order1 = createTestOrder(1L, OrderSide.SELL, BigDecimal(10), 1L) // 1 価格
        val order2 = createTestOrder(2L, OrderSide.SELL, BigDecimal(20), 2L) // 2 価格、時刻
        val order3 = createTestOrder(3L, OrderSide.SELL, BigDecimal(20), 3L) // 3 価格、時刻
        val order4 = createTestOrder(4L, OrderSide.SELL, BigDecimal(30), 4L) // 4 価格
        val buyOrders = sortedSetOf<Order>(order1, order2, order3, order4)
        Assertions.assertThat(buyOrders.toList())
            .`as`("価格、作成時刻による順序")
            .extracting<Long> { it.orderId }
            .containsExactly(1L, 2L, 3L, 4L)
    }
}

fun createTestOrder(orderId: Long, orderSide: OrderSide, price: BigDecimal, createdAtNs: Long): Order {
    return Order(
        CurrencyPair.BTC_JPY, // 通貨ペア
        orderId, // 注文ID
        OrderActive.ACTIVE, // 完了しているか
        1L, // ユーザID
        orderSide, // 売買
        OrderType.LIMIT, // 注文の方法
        OrderStatus.UNFILLED, // 注文の状態
        price, // 価格
        BigDecimal.ZERO, // 平均価格
        BigDecimal.ONE, // 数量
        BigDecimal.ONE, // 残数量
        TradeAction.MAKER, // メイカーテイカー
        "", // 処理ID
        1L, // 更新日時ns
        createdAtNs, // 作成日時ns
        BigDecimal.ZERO, // 変更数量
        ActionRequest.NONE, // 操作要求
        ActionResult.YET, // 操作結果
    )
}