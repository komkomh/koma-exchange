package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.*
import com.example.komaexchange.repositories.*
import io.andrewohara.dynamokt.DataClassTableSchema
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import java.math.BigDecimal
import java.util.*

private val dataClassTableSchema = DataClassTableSchema(Order::class)

class TradeWorker(shardMaster: ShardMaster, maxCount: Int = 99) : Worker<Order>(shardMaster) {
    private var assetCache = mutableMapOf<Long, Asset>() // userIdで資産をキャッシュする
    private var activeOrderCache = mutableSetOf<Order>()
    private var orderExecutor = OrderExecutor(maxCount = maxCount)
    override fun getTableSchema(): TableSchema<Order> {
        return orderTable.tableSchema()
//        return orderTableSchema
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
                        ShardMasterRepository.save(shardMaster.createDone())
                        QueueOrder.QUIT
                    } // これまで分を確定する
                    TransactionResult.FAILURE -> QueueOrder.RESET // 注文再送を依頼する
                }
            }
        }
    }

    fun insert(order: Order): QueueOrder {
        println("${System.currentTimeMillis()} : insert1 ")
        // キャッシュが空なら
        if (assetCache.isEmpty() && activeOrderCache.isEmpty()) {
            // キャッシュを取得する
            activeOrderCache =
                OrderRepository.findActive(order.currencyPair).filter { it.sequenceNumber != null }.toMutableSet()
            if (assetCache[order.userId] == null) {
                assetCache[order.userId] = AssetRepository.findOne(order.userId)
            }
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
        println("${System.currentTimeMillis()} : modify1 ")
        // TODO 実装
        return QueueOrder.CONTINUE
    }

    fun remove(order: Order): QueueOrder {
        println("${System.currentTimeMillis()} : remove1 ")
        orderExecutor.orders.removeIf { it.orderId == order.orderId }
        // TODO 実装
        return QueueOrder.CONTINUE
    }

    fun saveAndMerge(): TransactionResult {
        if (!orderExecutor.tradeResultValues.hasValue()) {
            return TransactionResult.SUCCESS
        }

        val newShardMaster = shardMaster.copy(
            sequenceNumber = orderExecutor.tradeResultValues.orders.map { it.sequenceNumber }.filterNotNull().max(),
            shardStatus = ShardStatus.RUNNING,
            lockedMs = System.currentTimeMillis(),
        )

        val result = TradeWorkerRepository.saveTransaction(
            orderExecutor.tradeResultValues.orders,
            orderExecutor.tradeResultValues.trades,
            orderExecutor.tradeResultValues.assets.map { Pair(assetCache[it.userId]!!, it) }.toSet(),
            newShardMaster,
        )
        if (result == TransactionResult.SUCCESS) {
            orderExecutor.assetMap.forEach { assetCache[it.key] = it.value.copy() } // 最新資産をマージする
            activeOrderCache.clear()
            orderExecutor.orders.forEach { activeOrderCache.add(it.copy()) } // 最新注文をマージする
        }
        orderExecutor.init(assetCache, activeOrderCache)
        return result
    }
}

data class OrderExecutor(
    val assetMap: MutableMap<Long, Asset> = mutableMapOf(),
    val orders: MutableSet<Order> = sortedSetOf(),
    var tradeResultValues: TradeResultValues = TradeResultValues(mutableSetOf(), mutableSetOf(), mutableSetOf()),
    val maxCount: Int,
) {
    fun trade(order: Order): TradeResult {

        // TODO 大口個客が全喰いするかチェックする -> する場合はキャンセルとする

        var takerOrder = order
        while (true) {
            val makerOrder = orders
                .filter { it.tradeAction == TradeAction.MAKER }
                .find { it.orderSide != takerOrder.orderSide }
            if (makerOrder == null) {
                val newOrder = when (takerOrder.orderType) {
                    OrderType.MARKET, OrderType.POST_ONLY -> takerOrder.createCanceledOrder() // takerをキャンセルする
                    OrderType.LIMIT -> takerOrder.copy(tradeAction = TradeAction.MAKER) // MAKERとする
                }

                val newTradeResult = TradeResultValues(mutableSetOf(newOrder), mutableSetOf(), mutableSetOf())
                when (tradeResultValues.merge(newTradeResult)) {
                    true -> {
                        merge()
                        break
                    }

                    false -> return TradeResult.FULL
                }
            }
            val newTradeResult = oneTrade(takerOrder, makerOrder) ?: break
            when (tradeResultValues.merge(newTradeResult)) {
                true -> {
                    merge()
                    takerOrder = newTradeResult.findTakerOrder() ?: break
                }

                false -> return TradeResult.FULL
            }
        }
        return TradeResult.NOT_FULL
    }

    fun init(assetCache: Map<Long, Asset>, orderCache: Set<Order>) {
        assetMap.clear()
        assetCache.forEach { (userId, asset) -> assetMap[userId] = asset.copy() }
        orders.clear()
        orders.addAll(orderCache.map { it.copy() })
        tradeResultValues = TradeResultValues(mutableSetOf(), mutableSetOf(), mutableSetOf())
    }

    private fun cacheAndGetAsset(userId: Long): Asset {
        if (!assetMap.containsKey(userId)) {
            assetMap[userId] = AssetRepository.findOne(userId)
        }
        return assetMap[userId]!!
    }

    fun merge() {
        tradeResultValues.assets.forEach { assetMap.put(it.userId, it.copy()) }
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
                makerOrder.price!!
            )
        ) {
            // 何もしない TODO ちがう
            return null
        }

        val takerAsset = cacheAndGetAsset(takerOrder.userId)
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
            mutableSetOf(newTakerOrder, newMakerOrder),
            mutableSetOf(takerTrade, makerTrade),
            mutableSetOf(takerAsset, makerAsset)
        )
    }
}

data class TradeResultValues(
    val orders: MutableSet<Order>,
    val trades: MutableSet<Trade>,
    val assets: MutableSet<Asset>,
) {
    private fun count(orderExecuteResult: TradeResultValues): Int {
        val orderCount = orderExecuteResult.orders.count { !orders.contains(it) }
        val tradeCount = orderExecuteResult.trades.count { !trades.contains(it) }
        val assetCount = orderExecuteResult.assets.count { !assets.contains(it) }
        return assets.size + orders.size + trades.size + orderCount + tradeCount + assetCount
    }

    fun merge(orderExecuteResult: TradeResultValues): Boolean { // TODO マージ方法が悪い
        return when (count(orderExecuteResult) < 100) {
            true -> {
                orders.removeAll(orderExecuteResult.orders)
                orders.addAll(orderExecuteResult.orders)
                trades.removeAll(orderExecuteResult.trades)
                trades.addAll(orderExecuteResult.trades)
                assets.removeAll(orderExecuteResult.assets)
                assets.addAll(orderExecuteResult.assets)
                true
            }

            false -> false
        }
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


