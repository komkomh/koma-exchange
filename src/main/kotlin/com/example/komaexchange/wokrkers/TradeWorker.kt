package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.*
import com.example.komaexchange.repositories.*
import software.amazon.awssdk.enhanced.dynamodb.Expression
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues
import software.amazon.awssdk.enhanced.dynamodb.model.TransactUpdateItemEnhancedRequest
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest
import java.math.BigDecimal
import java.util.*

class TradeWorker(shardMaster: ShardMaster) : Worker<Order>(shardMaster) {
    private var assetCache = mutableMapOf<Long, Asset>() // userIdで資産をキャッシュする
    private var activeOrderCache = mutableSetOf<Order>()
    private var tradeExecutor = TradeExecutor()
    override fun getTableSchema(): TableSchema<Order> {
        return orderTable.tableSchema()
    }

    // TODO 複数通過ペア対応
    // レコードが挿入されたことを検出した
    override fun recordInserted(order: Order): Transaction {
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
            tradeExecutor.init(assetCache, activeOrderCache)
        }

        // 残takerOrderがあれば約定する
        val remainingOrder = tradeExecutor.orders.find { it.tradeAction == TradeAction.TAKER }
        var tradeResult = when (remainingOrder) {
            null -> TradeResult.NOT_FULL
            else -> tradeExecutor.trade(remainingOrder)
        }

        // 今回対象注文を約定する
        tradeResult = when (tradeResult) {
            TradeResult.NOT_FULL -> tradeExecutor.trade(order)
            TradeResult.FULL -> TradeResult.FULL
        }

        return when (tradeResult) {
            TradeResult.NOT_FULL -> Transaction(QueueOrder.CONTINUE) // 一括更新アイテム数に余裕があるため次の注文を取得する
            TradeResult.FULL -> createTransaction(QueueOrder.UNTIL_DONE) // 一括更新アイテム数いっぱいになれば更新する
        }
    }

    // レコードが更新されたことを検出した
    override fun recordModified(order: Order): Transaction {
        println("${System.currentTimeMillis()} : modify1 ")
        // TODO 実装
        return Transaction(QueueOrder.CONTINUE)
    }

    // レコードが削除されたことを検出した
    override fun recordRemoved(order: Order): Transaction {
        println("${System.currentTimeMillis()} : remove1 ")
        // 注文が削除されることは本来ありえない
        tradeExecutor.orders.remove(order)
        return createTransaction(QueueOrder.CONTINUE)
    }

    // レコードへの操作が検出されなかった
    override fun recordNone(): Transaction {
        return createTransaction(QueueOrder.DONE)
    }

    // シャード受信の終了が検出された(このworkerも終わる)
    override fun recordFinished(): Transaction {
        // TODO どうするか
        // ShardMasterRepository.save(shardMaster.createDone())
        return createTransaction(QueueOrder.QUIT)
    }

    private fun createTransaction(queueOrder: QueueOrder): Transaction {
        println("saveTransaction: size = ${tradeExecutor.resultItems.orders.size + tradeExecutor.resultItems.trades.size + tradeExecutor.resultItems.assets.size}")
        val requestBuilder = TransactWriteItemsEnhancedRequest.builder()
        tradeExecutor.resultItems.orders.forEach { requestBuilder.addPutItem(orderTable, it) }
        tradeExecutor.resultItems.trades.forEach { requestBuilder.addPutItem(tradeTable, it) }
        tradeExecutor.resultItems.assets.forEach {
            val oldAsset = assetCache[it.userId]!!
            // 資産が変更されていればrollback
            requestBuilder.addUpdateItem(
                assetTable, TransactUpdateItemEnhancedRequest.builder(Asset::class.java)
                    .conditionExpression(
                        Expression.builder()
                            .expression("#updatedAt = :updatedAt")
                            .putExpressionName("#updatedAt", "updatedAt")
                            .putExpressionValue(":updatedAt", AttributeValues.numberValue(oldAsset.updatedAt))
                            .build()
                    )
                    .item(it)
                    .build()
            )
        }
        return Transaction(
            queueOrder = queueOrder,
            builder = requestBuilder,
            successFun = {
                // 約定後の資産をマージする
                assetCache.putAll(tradeExecutor.assetMap)
                // 約定後の注文をマージする
                activeOrderCache.clear()
                activeOrderCache.addAll(tradeExecutor.orders)
                // 約定結果をクリアする
                tradeExecutor.resultItems.clear()
            },
            failureFun = {
                // 資産を取り直す
                val assets = AssetRepository.find(tradeExecutor.assetMap.keys)
                assetCache.putAll(assets.map { it.userId to it })
                // 約定実行を初期化する
                tradeExecutor.init(assetCache, activeOrderCache)
            }
        )
    }
}

data class TradeExecutor(
    val assetMap: MutableMap<Long, Asset> = mutableMapOf(),
    val orders: MutableSet<Order> = sortedSetOf(),
    val maxItemCount: Int = 100,
    var resultItems: TradeResultItems = TradeResultItems(
        mutableSetOf(), mutableSetOf(), mutableSetOf(), maxItemCount
    ),
) {
    // 初期化する
    fun init(assetCache: Map<Long, Asset>, orderCache: Set<Order>) {
        // 資産を初期化する
        assetMap.clear()
        assetMap.putAll(assetCache.map { it.key to it.value.copy() })

        // 注文を初期化する
        orders.clear()
        orders.addAll(orderCache.map { it.copy() })

        // 約定結果をクリアする
        resultItems.clear()
    }

    // 約定
    fun trade(order: Order): TradeResult {

        // TODO 大口個客が全喰いするかチェックする -> する場合はキャンセルとする

        var takerOrder = order
        while (true) {
            val makerOrder = orders
                .filter { it.tradeAction == TradeAction.MAKER }
                .find { it.orderSide != takerOrder.orderSide }

            val tradeResult = oneTrade(takerOrder, makerOrder)
            when (merge(tradeResult)) {
                true -> takerOrder = tradeResult.findTakerOrder() ?: return TradeResult.NOT_FULL
                false -> return TradeResult.FULL
            }
        }
    }

    // 約定させる
    private fun oneTrade(takerOrder: Order, makerOrder: Order?): TradeResultItems {

        // MAKERがなければ
        if (makerOrder == null) {
            val newOrder = when (takerOrder.orderType) {
                OrderType.MARKET, OrderType.POST_ONLY -> takerOrder.createCanceledOrder() // TAKERをキャンセルする
                OrderType.LIMIT -> takerOrder.copy(tradeAction = TradeAction.MAKER) // MAKERとする
            }
            return TradeResultItems(mutableSetOf(newOrder), mutableSetOf(), mutableSetOf(), maxItemCount)
        }

        // 指値でクロスしていないければ
        if (takerOrder.orderType == OrderType.LIMIT && !takerOrder.orderSide.isCross(
                takerOrder.price!!,
                makerOrder.price!!
            )
        ) {
            // MAKERとする
            val newOrder = takerOrder.copy(tradeAction = TradeAction.MAKER)
            return TradeResultItems(mutableSetOf(newOrder), mutableSetOf(), mutableSetOf(), maxItemCount)
        }

        // 対象数量
        val tradeAmount = when (takerOrder.remainingAmount < makerOrder.remainingAmount) {
            true -> takerOrder.remainingAmount
            false -> makerOrder.remainingAmount
        }

        // 資産を取得する
        val cacheAndGetAsset = fun(userId: Long): Asset {
            return when (val asset = assetMap[userId]) {
                null -> assetMap.put(userId, AssetRepository.findOne(userId))!!
                else -> asset
            }
        }
        val takerAsset = cacheAndGetAsset(takerOrder.userId)
        val makerAsset = cacheAndGetAsset(makerOrder.userId)
        // TODO assetを計算する

        val newTakerOrder = takerOrder.createTradedOrder(tradeAmount)
        val newMakerOrder = makerOrder.createTradedOrder(tradeAmount)
        val takerTradeId = UUID.randomUUID().toString()
        val makerTradeId = UUID.randomUUID().toString()
        val takerTrade = takerOrder.createTrade(tradeAmount, makerOrder.price!!, makerOrder, makerTradeId)
        val makerTrade = makerOrder.createTrade(tradeAmount, makerOrder.price, takerOrder, takerTradeId)
        return TradeResultItems(
            mutableSetOf(newTakerOrder, newMakerOrder),
            mutableSetOf(takerTrade, makerTrade),
            mutableSetOf(takerAsset, makerAsset),
            maxItemCount
        )
    }

    private fun merge(newResultItems: TradeResultItems): Boolean {

        if (resultItems.count(newResultItems) < maxItemCount) {
            // 約定結果を更新する
            resultItems.orders.removeAll(newResultItems.orders)
            resultItems.orders.addAll(newResultItems.orders)
            resultItems.trades.removeAll(newResultItems.trades)
            resultItems.trades.addAll(newResultItems.trades)
            resultItems.assets.removeAll(newResultItems.assets)
            resultItems.assets.addAll(newResultItems.assets)
            // 現在の約定実行のキャッシュを更新する
            assetMap.putAll(resultItems.assets.map { it.userId to it.copy() })
            orders.removeAll(resultItems.orders)
            orders.addAll(resultItems.orders.filter { it.orderActive == OrderActive.ACTIVE }.map { it.copy() })
            return true
        }
        return false
    }

}

data class TradeResultItems(
    val orders: MutableSet<Order>,
    val trades: MutableSet<Trade>,
    val assets: MutableSet<Asset>,
    val maxItemCount: Int,
) {
    fun clear() {
        orders.clear()
        trades.clear()
        assets.clear()
    }

    fun count(orderExecuteResult: TradeResultItems): Int {
        val orderCount = orderExecuteResult.orders.count { !orders.contains(it) }
        val tradeCount = orderExecuteResult.trades.count { !trades.contains(it) }
        val assetCount = orderExecuteResult.assets.count { !assets.contains(it) }
        return assets.size + orders.size + trades.size + orderCount + tradeCount + assetCount
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


