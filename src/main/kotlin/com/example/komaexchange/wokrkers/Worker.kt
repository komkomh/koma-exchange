package com.example.komaexchange.wokrkers

import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest
import kotlin.reflect.KClass

abstract class Worker<T : Any> {
    abstract fun getEntityClazz(): KClass<T>;
    abstract fun new(): Worker<T>;

    abstract fun insert(t: T): Transaction?
    abstract fun modify(t: T): Transaction?
    abstract fun remove(t: T): Transaction?
}

data class Transaction(
    val transactionRequestBuilder: TransactWriteItemsEnhancedRequest.Builder,
    val successFun: () -> Unit
) {
}