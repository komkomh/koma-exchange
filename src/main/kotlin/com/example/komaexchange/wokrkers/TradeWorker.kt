package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.*
import com.example.komaexchange.repositories.AssetRepository
import com.example.komaexchange.repositories.OrderRepository
import com.example.komaexchange.repositories.ShardMasterRepository
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.reflect.KClass

private val orderRepository = OrderRepository()
private val assetRepository = AssetRepository()
private val shardMasterRepository = ShardMasterRepository()

class OrderExecuteWorker : Worker<Order>() {
    private var assetCache = mutableMapOf<Long, Asset>() // userIdで資産をキャッシュする
    private var activeOrderCache = sortedSetOf<Order>()
    override fun getEntityClazz(): KClass<Order> {
        return Order::class
    }

    override fun new(): Worker<Order> {
        return OrderExecuteWorker();
    }

    // TODO cacheを毎回作るか、約定後に毎回反映させるかを決める -> 一旦、約定後に反映させるパターンで試してみる
    suspend fun execute() {


        var orderExecutor = OrderExecutor(assetCache, activeOrderCache) // TODO deep copy
        while (true) {
            val order = queue.peek()

            // TODO init処理(取得する)
            // TODO init処理(takerOrderを無くすまで処理する)

            val tradeResult = orderExecutor.trade(order)
            when (tradeResult) {
                TradeResult.FULL, TradeResult.ORDER_EMPTY -> {
                    // TODO トランザクション実行
                    // TODO cache更新
                    // queue reset
                    queue.rollback()
                    // TODO orderExecutor reset
                }
                TradeResult.MORE -> {

                }
                TradeResult.MAKER_NOT_FOUND -> {

                }
            }
        }
    }

    fun execute(orderExecutor: OrderExecutor) {

    }
}

data class OrderExecutor(
    var assetCache: MutableMap<Long, Asset>,
    var orderCache: MutableSet<Order>,
    var tradeResult: TradeResultValues = TradeResultValues(setOf(), setOf(), setOf()),
) {
    fun trade(order: Order?): TradeResult {

        if (order == null) {
            return TradeResult.ORDER_EMPTY
        }
        println("${System.currentTimeMillis()} : insert2 ")

        var takerOrder = order
        while (true) {
            val makerOrder = orderCache.find { it.orderSide != order.orderSide } ?: return TradeResult.MAKER_NOT_FOUND
            val newTradeResult = oneTrade(takerOrder, makerOrder) ?: break

            if (tradeResult.count(newTradeResult) > 99) {
                return TradeResult.FULL
            }
            this.tradeResult = tradeResult.merge(newTradeResult)
            refreshCache()
            takerOrder = newTradeResult.findTakerOrder() ?: return TradeResult.MORE
        }
        return TradeResult.MORE
    }

    private fun cacheAsset(userId: Long) {
        if (!assetCache.containsKey(userId)) {
            assetCache[userId] = assetRepository.findOne(userId)
        }
    }

    fun refreshCache() {
        tradeResult.assets.forEach { assetCache.plus(it.userId to it) }
        orderCache.removeAll(tradeResult.orders)
        orderCache.addAll(tradeResult.orders.filter { it.orderActive == OrderActive.ACTIVE }.toSet())
    }

    // 約定させる
    private fun oneTrade(
        takerOrder: Order,
        makerOrder: Order
    ): TradeResultValues? {
        // 残数量がなければ
        if (takerOrder.remainingAmount <= BigDecimal.ZERO) {
            // 何もしない
            return null
        }
        // 指値でクロスしていないければ
        if (takerOrder.orderType == OrderType.LIMIT && !takerOrder.orderSide.isCross(
                takerOrder.price,
                takerOrder.price
            )
        ) {
            // 何もしない TODO ちがう
            return null
        }

        cacheAsset(takerOrder.orderId)
        val takerAsset = assetCache[takerOrder.orderId]!!

        cacheAsset(makerOrder.userId)
        val makerAsset = assetCache[makerOrder.userId]!!

        // 対象数量
        val tradeAmount =
            if (takerOrder.remainingAmount < makerOrder.remainingAmount) takerOrder.remainingAmount else makerOrder.remainingAmount

        // TODO assetを計算する

        val newTakerOrder = takerOrder.createExecutionOrder(tradeAmount)
        val newMakerOrder = makerOrder.createExecutionOrder(tradeAmount)
        val takerTradeId = UUID.randomUUID().toString()
        val makerTradeId = UUID.randomUUID().toString()
        val takerTrade = takerOrder.createExecutionTrade(tradeAmount, makerOrder, makerTradeId)
        val makerTrade = makerOrder.createExecutionTrade(tradeAmount, takerOrder, takerTradeId)
        return TradeResultValues(
            setOf(newTakerOrder, newMakerOrder),
            setOf(takerTrade, makerTrade),
            setOf(takerAsset, makerAsset)
        )
    }
}

data class TradeResultValues(
    val orders: Set<Order>,
    var trades: Set<Trade>,
    val assets: Set<Asset>,
) {
    fun count(orderExecuteResult: TradeResultValues): Int {
        val orderCount = orderExecuteResult.orders.count { !orders.contains(it) }
        val tradeCount = orderExecuteResult.trades.count { !trades.contains(it) }
        val assetCount = orderExecuteResult.assets.count { !assets.contains(it) }
        return assets.size + orders.size + trades.size + orderCount + tradeCount + assetCount
    }

    fun merge(orderExecuteResult: TradeResultValues): TradeResultValues {
        return TradeResultValues(
            orders + orderExecuteResult.orders,
            trades + orderExecuteResult.trades,
            assets + orderExecuteResult.assets,
        )
    }

    fun findTakerOrder(): Order? {
        return orders.filter { it.tradeAction == TradeAction.TAKER }.find { it.orderActive == OrderActive.ACTIVE }
    }
}

enum class TradeResult {
    MORE, FULL, ORDER_EMPTY, MAKER_NOT_FOUND
}


