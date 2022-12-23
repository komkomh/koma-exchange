package com.example.komaexchange.maintenance


import com.example.komaexchange.entities.*
import com.example.komaexchange.repositories.AssetRepository
import com.example.komaexchange.repositories.OrderRepository
import java.math.BigDecimal

private val orderRepository = OrderRepository()
private val assetRepository = AssetRepository()

fun main() {
    println("order create!");
//    createAsset()
//    two()
    random()
}

fun createAsset() {
    val asset1 = Asset(
        Currency.JPY, // 通貨ペア
        10L, // ユーザID
        BigDecimal(1000000), // 資産
        BigDecimal(0), // ロック資産
        System.nanoTime(), // 更新日時NS
        System.nanoTime(), // 作成日時NS
    )
    assetRepository.save(asset1)
}

fun two() {
    val buyOrder = Order(
        CurrencyPair.BTC_JPY, // 通貨ペア
        5L, // 注文ID
        OrderActive.ACTIVE, // 完了しているか
        10L, // ユーザID
        OrderSide.SELL, // 売買
        OrderType.LIMIT, // 注文の方法
        OrderStatus.UNFILLED, // 注文の状態
        BigDecimal(10), // 価格
        BigDecimal(10), // 平均価格
        BigDecimal(1), // 数量
        BigDecimal(1), // 残数量
        TradeAction.TAKER, // メイカーテイカー
        "", // 処理ID
        System.nanoTime(), // 更新日時ns
        System.nanoTime(), // 作成日時ns
        BigDecimal.ZERO, // 変更数量
        ActionRequest.ORDER, // 操作要求
        ActionResult.YET, // 操作結果
    )
    orderRepository.save(buyOrder)

    val sellOrder = Order(
        CurrencyPair.BTC_JPY, // 通貨ペア
        6L, // 注文ID
        OrderActive.ACTIVE, // 完了しているか
        10L, // ユーザID
        OrderSide.BUY, // 売買
        OrderType.LIMIT, // 注文の方法
        OrderStatus.UNFILLED, // 注文の状態
        BigDecimal(10), // 価格
        BigDecimal(10), // 平均価格
        BigDecimal(1), // 数量
        BigDecimal(1), // 残数量
        TradeAction.TAKER, // メイカーテイカー
        "", // 処理ID
        System.nanoTime(), // 更新日時ns
        System.nanoTime(), // 作成日時ns
        BigDecimal.ZERO, // 変更数量
        ActionRequest.ORDER, // 操作要求
        ActionResult.YET, // 操作結果
    )
    orderRepository.save(sellOrder)
}

fun random() {
    println("hello!");
    (0L..1000L).forEach {
        val order = Order(
            CurrencyPair.BTC_JPY, // 通貨ペア
            it, // 注文ID
            OrderActive.ACTIVE, // 完了しているか
            10L, // ユーザID
            OrderSide.values().random(), // 売買
            OrderType.LIMIT, // 注文の方法
            OrderStatus.UNFILLED, // 注文の状態
            BigDecimal(10), // 価格
            BigDecimal(10), // 平均価格
            BigDecimal(1), // 数量
            BigDecimal(1), // 残数量
            TradeAction.TAKER, // メイカーテイカー
            "", // 処理ID
            System.nanoTime(), // 更新日時ns
            System.nanoTime(), // 作成日時ns
            BigDecimal.ZERO, // 変更数量
            ActionRequest.ORDER, // 操作要求
            ActionResult.YET, // 操作結果
        )
        orderRepository.save(order)
    }
}

