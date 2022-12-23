package com.example.komaexchange.tools

import com.example.komaexchange.repositories.AssetRepository
import com.example.komaexchange.repositories.OrderRepository
import com.example.komaexchange.repositories.ShardMasterRepository
import com.example.komaexchange.repositories.TradeRepository

fun main() {

    ShardMasterRepository().createTable()
    AssetRepository().createTable()
    OrderRepository().createTable()
    TradeRepository().createTable()
}

