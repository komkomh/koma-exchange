package com.example.komaexchange.wokrkers

import com.example.komaexchange.entities.*
import com.example.komaexchange.repositories.AssetRepository
import com.example.komaexchange.repositories.OrderRepository
import java.math.BigDecimal
import java.util.*
import kotlin.reflect.KClass

class OrderExecuteWorker : Worker<Order>() {
    private val orderRepository = OrderRepository()
    private val assetRepository = AssetRepository()
    private var buyOrders = sortedSetOf<Order>()
    private var sellOrders = sortedSetOf<Order>()
    override fun getEntityClazz(): KClass<Order> {
        return Order::class
    }

    override fun new(): Worker<Order> {
        return OrderExecuteWorker();
    }

    override fun insert(t: Order): Transaction {
        // 初回であれば
        if (buyOrders.isEmpty() && sellOrders.isEmpty()) {
            // DynamoDBから有効な注文を取得し、makerに絞る
            val activeMakerOrders = orderRepository.findActive(t.currencyPair, OrderActive.ACTIVE)
                .filter { it.tradeAction == TradeAction.MAKER }
            buyOrders.addAll(activeMakerOrders.filter { it.orderSide == OrderSide.BUY })
            sellOrders.addAll(activeMakerOrders.filter { it.orderSide == OrderSide.SELL })
        }

        println("${System.currentTimeMillis()} : insert2 ")
        val oldAsset = assetRepository.findOne(t.currencyPair.quote, t.userId)
        println("${System.currentTimeMillis()} : insert3 ")
        val newAsset = oldAsset.execution(t)
            ?: return Transaction(
                orderRepository.createCancelRequest(t.createCanceledOrder()),
                fun() {}
            )

        val activeOrders = getActiveOrders(t.orderSide)
        val results = mutableListOf<OrderExecuteResult>()
        var takerOrder = t
        for (makerOrder in activeOrders) {
            val result = execute(takerOrder, makerOrder) ?: break
            takerOrder = result.takerOrder
            results += result
        }

        val executionOrders = results.map { it.makerOrder } + takerOrder.toMakerOrder()
        val executionTrades = results.map { it.takerTrade } + results.map { it.makerTrade }

        val transactionRequest = orderRepository.createExecutionRequest(
            executionOrders,
            executionTrades,
            oldAsset,
            newAsset
        )
        val successFun = fun() {
            val buyExecutionOrders = executionOrders.filter { it.orderSide == OrderSide.BUY }
            val sellExecutionOrders = executionOrders.filter { it.orderSide == OrderSide.SELL }

            // 約定済み注文をキャッシュから除去するる
            buyOrders.removeAll(buyExecutionOrders.filter { it.orderActive == OrderActive.IN_ACTIVE }.toSet())
            sellOrders.removeAll(sellExecutionOrders.filter { it.orderActive == OrderActive.IN_ACTIVE }.toSet())

            // 新規注文をキャッシュに追加＆部分約定注文をキャッシュに更新する
            buyOrders.addAll(buyExecutionOrders.filter { it.orderActive == OrderActive.ACTIVE }.toSet())
            sellOrders.addAll(sellExecutionOrders.filter { it.orderActive == OrderActive.ACTIVE }.toSet())
        }
        return Transaction(transactionRequest, successFun)
    }

    override fun modify(t: Order): Transaction? {
        println("${System.currentTimeMillis()} : modify ")
        if (buyOrders.contains(t)) {
            buyOrders.add(t)
        }
        if (sellOrders.contains(t)) {
            sellOrders.add(t)
        }
        return null
    }

    override fun remove(t: Order): Transaction? {
        println("${System.currentTimeMillis()} : remove ")
        buyOrders.remove(t)
        sellOrders.remove(t)
        return null
    }

    private fun getActiveOrders(orderSide: OrderSide): MutableSet<Order> {
        return when (orderSide) {
            OrderSide.BUY -> sellOrders
            OrderSide.SELL -> buyOrders
        }
    }

    // 約定させる
    fun execute(taker: Order, maker: Order): OrderExecuteResult? {
        // 残数量がなければ
        if (taker.remainingAmount <= BigDecimal.ZERO) {
            // 何もしない
            return null
        }
        // 指値でクロスしていないければ
        if (taker.orderType == OrderType.LIMIT && !taker.orderSide.isCross(taker.price, maker.price)) {
            // 何もしない
            return null
        }
        // 対象数量
        val tradeAmount =
            if (taker.remainingAmount < maker.remainingAmount) taker.remainingAmount else maker.remainingAmount

        val takerOrder = taker.createExecutionOrder(tradeAmount)
        val makerOrder = maker.createExecutionOrder(tradeAmount)
        val takerTradeId = UUID.randomUUID().toString()
        val makerTradeId = UUID.randomUUID().toString()
        val takerTrade = taker.createExecutionTrade(tradeAmount, makerOrder, makerTradeId)
        val makerTrade = maker.createExecutionTrade(tradeAmount, takerOrder, takerTradeId)
        return OrderExecuteResult(takerOrder, makerOrder, takerTrade, makerTrade)
    }
}

data class OrderExecuteResult(
    val takerOrder: Order,
    val makerOrder: Order,
    var takerTrade: Trade,
    var makerTrade: Trade,
) {
}

