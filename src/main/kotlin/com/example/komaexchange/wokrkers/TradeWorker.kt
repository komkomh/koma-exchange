package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.*
import com.example.komaexchange.repositories.AssetRepository
import com.example.komaexchange.repositories.OrderRepository
import com.example.komaexchange.repositories.OrderWorkerRepository
import com.example.komaexchange.repositories.ShardMasterRepository
import java.math.BigDecimal
import java.util.*
import kotlin.reflect.KClass

private val orderRepository = OrderRepository()
private val assetRepository = AssetRepository()
private val shardMasterRepository = ShardMasterRepository()
private val orderWorkerRepository = OrderWorkerRepository()

class OrderExecuteWorker() : Worker<Order>() {
    private var assetCache = mapOf<Long, Asset>() // userIdで資産をキャッシュする
    private var activeOrderCache = setOf<Order>()
    private var orderExecutor = OrderExecutor()
    override fun getEntityClazz(): KClass<Order> {
        return Order::class
    }

    override fun new(): Worker<Order> {
        return OrderExecuteWorker();
    }

    // TODO 複数通過ペア対応
    override fun execute(t: Order?): QueueOrder {

        // 注文がなければ
        if (t == null) {
            return when (saveAndMerge()) {
                TransactionResult.SUCCESS -> QueueOrder.DONE // これまで分を確定する
                TransactionResult.FAILURE -> QueueOrder.RESET // 注文再送を依頼する
            }
        }

        // キャッシュが空なら
        if (activeOrderCache.isEmpty()) {
            // キャッシュを取得する
            activeOrderCache =
                orderRepository.findActive(t.currencyPair).filter { it.sequenceNumber != null }.toSet()
            // 約定実行を初期化する
            orderExecutor.init(assetCache, activeOrderCache)
        }

        // 残takerOrderがあれば対象注文とする
        val remainingTakerOrder = activeOrderCache.find { it.tradeAction == TradeAction.TAKER }
        if (remainingTakerOrder != null) {
            if (orderExecutor.trade(remainingTakerOrder) == TradeResult.FULL) { // 一括更新アイテム数いっぱいになれば
                return when (saveAndMerge()) { // DBに反映する
                    TransactionResult.SUCCESS -> QueueOrder.UNTIL_DONE // 前回分までを確定する
                    TransactionResult.FAILURE -> QueueOrder.RESET // 注文再送を依頼する
                }
            }
        }

        // 注文を約定する
        return when (orderExecutor.trade(t)) {
            TradeResult.FULL -> { // 一括更新アイテム数いっぱいになれば
                when (saveAndMerge()) { // DBに反映する
                    TransactionResult.SUCCESS -> QueueOrder.UNTIL_DONE // 前回分までを確定する
                    TransactionResult.FAILURE -> QueueOrder.RESET // 注文再送を依頼する
                }
            }
            // 一括更新アイテム数に余裕があるため
            TradeResult.NOT_FULL -> QueueOrder.CONTINUE // 次の注文を取得する
        }
    }

    fun saveAndMerge(): TransactionResult {
        if (!orderExecutor.tradeResultValues.hasValue()) {
            return TransactionResult.SUCCESS
        }

        val result = orderWorkerRepository.saveTransaction(
            orderExecutor.tradeResultValues.orders,
            orderExecutor.tradeResultValues.trades,
            orderExecutor.tradeResultValues.assets.map { Pair(assetCache[it.userId]!!, it) }.toSet(),
            shardMaster
        )
        if (result == TransactionResult.SUCCESS) {
            assetCache = orderExecutor.assetMap // 最新資産をマージする
            activeOrderCache = orderExecutor.orders // 最新注文をマージする
        }
        orderExecutor.init(assetCache, activeOrderCache)
        return result
    }
}

data class OrderExecutor(
    val assetMap: MutableMap<Long, Asset> = mutableMapOf(),
    val orders: MutableSet<Order> = mutableSetOf(),
    var tradeResultValues: TradeResultValues = TradeResultValues(setOf(), setOf(), setOf()),
) {
    fun trade(order: Order): TradeResult {

        println("${System.currentTimeMillis()} : insert2 ")
        // TODO 大口個客が全喰いするかチェックする -> する場合はキャンセルとする

        var takerOrder = order
        while (true) {
            val makerOrder = orders.find { it.orderSide != order.orderSide }
            if (makerOrder == null) {
                // takerをキャンセルする
                this.tradeResultValues = tradeResultValues.merge(
                    TradeResultValues(setOf(takerOrder.createCanceledOrder()), setOf(), setOf())
                )
                return TradeResult.NOT_FULL
            }
            val newTradeResult = oneTrade(takerOrder, makerOrder) ?: break

            if (tradeResultValues.count(newTradeResult) > 99) {
                return TradeResult.FULL
            }
            this.tradeResultValues = tradeResultValues.merge(newTradeResult)
            merge()
            takerOrder = newTradeResult.findTakerOrder() ?: return TradeResult.NOT_FULL
        }
        return TradeResult.NOT_FULL
    }

    fun init(assetCache: Map<Long, Asset>, orderCache: Set<Order>) {
        assetMap.clear()
        assetMap.forEach { (userId, asset) -> assetMap[userId] = asset.copy() }
        orders.clear()
        orders.addAll(orderCache.map { it.copy() })
        tradeResultValues = TradeResultValues(setOf(), setOf(), setOf())
    }

    private fun cacheAndGetAsset(userId: Long): Asset {
        if (!assetMap.containsKey(userId)) {
            assetMap[userId] = assetRepository.findOne(userId)
        }
        return assetMap[userId]!!
    }

    fun merge() {
        tradeResultValues.assets.forEach { assetMap.plus(it.userId to it) }
        orders.removeAll(tradeResultValues.orders)
        orders.addAll(tradeResultValues.orders.filter { it.orderActive == OrderActive.ACTIVE }.toSet())
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

        val takerAsset = cacheAndGetAsset(takerOrder.orderId)
        val makerAsset = cacheAndGetAsset(makerOrder.userId)

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
    val trades: Set<Trade>,
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

    fun hasValue(): Boolean {
        return orders.isNotEmpty() || trades.isNotEmpty()
    }
}

enum class TradeResult {
    NOT_FULL, FULL
}


