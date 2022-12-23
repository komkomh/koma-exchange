package com.example.komaexchange.workers

import com.example.komaexchange.entities.*
import com.example.komaexchange.wokrkers.OrderExecuteWorker
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class TradeWorkerTest {
    @Test
    @DisplayName("約定なし")
    fun tradeNullTest() {
        // 準備
        val tradeWorker = OrderExecuteWorker()
        val taker = createTestOrder(1L, OrderSide.BUY, BigDecimal(10), 1L)
        val maker = createTestOrder(2L, OrderSide.SELL, BigDecimal(20), 2L)

        val result = tradeWorker.execute(taker, maker);
        Assertions.assertThat(result).`as`("指値書10, 売20で約定しない").isNull()
    }

    @Test
    @DisplayName("指値で約定")
    fun tradeLimitTest() {
        // 準備
        val tradeWorker = OrderExecuteWorker()
        val taker = Order(
            CurrencyPair.BTC_JPY, // 通貨ペア
            1L, // 注文ID
            OrderActive.ACTIVE, // 完了しているか
            1L, // ユーザID
            OrderSide.BUY, // 売買
            OrderType.LIMIT, // 注文の方法
            OrderStatus.UNFILLED, // 注文の状態
            BigDecimal(20), // 価格
            BigDecimal.ZERO, // 平均価格
            BigDecimal.ONE, // 数量
            BigDecimal.ONE, // 残数量
            TradeAction.TAKER, // メイカーテイカー
            "", // 処理ID
            0L, // 更新日時ns
            1L, // 作成日時ns
            BigDecimal.ZERO, // 変更数量
            ActionRequest.ORDER, // 操作要求
            ActionResult.YET, // 操作結果
        )
        val maker = Order(
            CurrencyPair.BTC_JPY, // 通貨ペア
            1L, // 注文ID
            OrderActive.ACTIVE, // 完了しているか
            1L, // ユーザID
            OrderSide.SELL, // 売買
            OrderType.LIMIT, // 注文の方法
            OrderStatus.PARTIALLY_FILLED, // 注文の状態
            BigDecimal(20), // 価格
            BigDecimal.ZERO, // 平均価格
            BigDecimal.ONE, // 数量
            BigDecimal.ONE, // 残数量
            TradeAction.MAKER, // メイカーテイカー
            "", // 処理ID
            1L, // 更新日時ns
            1L, // 作成日時ns
            BigDecimal.ZERO, // 変更数量
            ActionRequest.NONE, // 操作要求
            ActionResult.ORDER_SUCCEEDED, // 操作結果
        )

        val result = tradeWorker.execute(taker, maker);
        Assertions.assertThat(result!!.takerOrder).isEqualTo(
            Order(
                CurrencyPair.BTC_JPY, // 通貨ペア
                1L, // 注文ID
                OrderActive.IN_ACTIVE, // 完了しているか
                1L, // ユーザID
                OrderSide.BUY, // 売買
                OrderType.LIMIT, // 注文の方法
                OrderStatus.FULLY_FILLED, // 注文の状態
                BigDecimal(10), // 価格
                BigDecimal.ZERO, // 平均価格
                BigDecimal.ONE, // 数量
                BigDecimal.ONE, // 残数量
                TradeAction.MAKER, // メイカーテイカー
                "", // 処理ID
                1L, // 更新日時ns
                1L, // 作成日時ns
                BigDecimal.ZERO, // 変更数量
                ActionRequest.NONE, // 操作要求
                ActionResult.ORDER_SUCCEEDED, // 操作結果
            )
        )
    }
}

fun createTestOrder(orderId: Long, orderSide: OrderSide, price: BigDecimal, createdAtNs: Long): Order  {
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