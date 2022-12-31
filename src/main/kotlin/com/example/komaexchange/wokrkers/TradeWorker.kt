package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.*
import com.example.komaexchange.repositories.AssetRepository
import com.example.komaexchange.repositories.OrderRepository
import com.example.komaexchange.repositories.ShardMasterRepository
import com.example.komaexchange.repositories.TradeWorkerRepository
import java.math.BigDecimal
import java.util.*
import kotlin.reflect.KClass

private val orderRepository = OrderRepository()
private val assetRepository = AssetRepository()
private val tradeRepository = TradeWorkerRepository()
private val shardMasterRepository = ShardMasterRepository()

class TradeWorker(shardMaster: ShardMaster, maxCount: Int = 99) : Worker<Order>(shardMaster) {
    private var assetCache = mapOf<Long, Asset>() // userIdで資産をキャッシュする
    private var activeOrderCache = setOf<Order>()
    private var orderExecutor = OrderExecutor(maxCount = maxCount)
    override fun getEntityClazz(): KClass<Order> {
        return Order::class
    }

    // TODO 複数通過ペア対応
    override fun execute(record: Record<Order>): QueueOrder {
        return when (record) {
            is Record.INSERTED -> insert(record.t.copy(sequenceNumber = record.sequenceNumber))
            is Record.MODIFIED -> modify(record.t)
            is Record.REMOVED -> remove(record.t)
            is Record.NONE -> {
                return when (saveAndMerge()) {
                    TransactionResult.SUCCESS -> QueueOrder.DONE // これまで分を確定する
                    TransactionResult.FAILURE -> QueueOrder.RESET // 注文再送を依頼する
                }
            }
            is Record.FINISHED -> {
                return when (saveAndMerge()) {
                    TransactionResult.SUCCESS -> {
                        shardMasterRepository.save(shardMaster.createDone())
                        QueueOrder.QUIT
                    } // これまで分を確定する
                    TransactionResult.FAILURE -> QueueOrder.RESET // 注文再送を依頼する
                }
            }
        }
    }

    fun insert(order: Order): QueueOrder {
        // キャッシュが空なら
        if (activeOrderCache.isEmpty()) {
            // キャッシュを取得する
            activeOrderCache =
                orderRepository.findActive(order.currencyPair).filter { it.sequenceNumber != null }.toSet()
            // 約定実行を初期化する
            orderExecutor.init(assetCache, activeOrderCache)
        }

        // 残takerOrderがあれば約定する
        val remainingOrder = orderExecutor.orders.find { it.tradeAction == TradeAction.TAKER }
        if (remainingOrder != null) {
            if (orderExecutor.trade(remainingOrder) == TradeResult.FULL) { // 一括更新アイテム数いっぱいになれば
                return when (saveAndMerge()) { // DBに反映する
                    TransactionResult.SUCCESS -> QueueOrder.UNTIL_DONE // 前回分までを確定する
                    TransactionResult.FAILURE -> QueueOrder.RESET // 注文再送を依頼する
                }
            }
        }

        // 注文を約定する
        return when (orderExecutor.trade(order)) {
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

    fun modify(order: Order): QueueOrder {
        // TODO 実装
        return QueueOrder.CONTINUE
    }

    fun remove(order: Order): QueueOrder {
        // TODO 実装
        return QueueOrder.CONTINUE
    }

    fun saveAndMerge(): TransactionResult {
        if (!orderExecutor.tradeResultValues.hasValue()) {
            return TransactionResult.SUCCESS
        }

        val newShardMaster = shardMaster.copy(
            sequenceNumber = orderExecutor.tradeResultValues.orders.map { it.sequenceNumber }.filterNotNull().max(),
            lockedMs = System.currentTimeMillis(),
        )

        val result = tradeRepository.saveTransaction(
            orderExecutor.tradeResultValues.orders,
            orderExecutor.tradeResultValues.trades,
            orderExecutor.tradeResultValues.assets.map { Pair(assetCache[it.userId]!!, it) }.toSet(),
            newShardMaster,
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
    val orders: MutableSet<Order> = sortedSetOf(),
    var tradeResultValues: TradeResultValues = TradeResultValues(setOf(), setOf(), setOf()),
    val maxCount: Int,
) {
    fun trade(order: Order): TradeResult {

        println("${System.currentTimeMillis()} : insert2 ")
        // TODO 大口個客が全喰いするかチェックする -> する場合はキャンセルとする

        var takerOrder = order
        while (true) {
            val makerOrder = orders
                .filter { it.tradeAction == TradeAction.MAKER }
                .find { it.orderSide != takerOrder.orderSide }
            if (makerOrder == null) {
                // takerをキャンセルする
                this.tradeResultValues = tradeResultValues.merge(
                    TradeResultValues(setOf(takerOrder.createCanceledOrder()), setOf(), setOf())
                )
                return TradeResult.NOT_FULL
            }
            val newTradeResult = oneTrade(takerOrder, makerOrder) ?: break

            if (tradeResultValues.count(newTradeResult) > maxCount) {
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
        assetCache.forEach { (userId, asset) -> assetMap[userId] = asset.copy() }
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
    private fun oneTrade(takerOrder: Order, makerOrder: Order): TradeResultValues? {

        // 残数量がなければ
        if (takerOrder.remainingAmount <= BigDecimal.ZERO) {
            // 何もしない
            return null
        }

        // 指値でクロスしていないければ
        if (takerOrder.orderType == OrderType.LIMIT && !takerOrder.orderSide.isCross(
                takerOrder.price!!,
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

        val newTakerOrder = takerOrder.createTradedOrder(tradeAmount)
        val newMakerOrder = makerOrder.createTradedOrder(tradeAmount)
        val takerTradeId = UUID.randomUUID().toString()
        val makerTradeId = UUID.randomUUID().toString()
        val takerTrade = takerOrder.createTrade(tradeAmount, makerOrder.price!!, makerOrder, makerTradeId)
        val makerTrade = makerOrder.createTrade(tradeAmount, makerOrder.price, takerOrder, takerTradeId)
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


