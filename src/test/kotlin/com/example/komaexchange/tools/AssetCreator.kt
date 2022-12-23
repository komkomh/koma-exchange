//package com.example.komaexchange.maintenance
//
//import com.example.helloexchangekotlin.entities.Asset
//import com.example.helloexchangekotlin.entities.Currency
//import com.example.helloexchangekotlin.repositories.AssetRepository
//import java.math.BigDecimal
//
//fun main() {
//    val assetRepository = AssetRepository()
//    val asset = Asset(
//        Currency.JPY, // 通貨ペア
//        10L, // ユーザID
//        BigDecimal(100000), // 資産
//        BigDecimal(0), // ロック資産
//        System.nanoTime(), // 更新日時NS
//        System.nanoTime(), // 作成日時NS
//    )
//    assetRepository.save(asset)
//}
//
