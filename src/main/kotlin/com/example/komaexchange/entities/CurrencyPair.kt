package com.example.komaexchange.entities

enum class CurrencyPair(val base: Currency, val quote: Currency) {
    BTC_JPY(Currency.BTC, Currency.JPY),
    ETH_JPY(Currency.ETH, Currency.JPY);
}

enum class Currency {
    BTC, ETH, JPY
}