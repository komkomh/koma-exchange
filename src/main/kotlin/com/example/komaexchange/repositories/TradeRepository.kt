package com.example.komaexchange.repositories

import io.andrewohara.dynamokt.createTableWithIndices

object xx

object TradeRepository {
    fun createTable() {
        tradeTable.createTableWithIndices()
    }
}