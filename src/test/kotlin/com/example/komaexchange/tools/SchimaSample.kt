//package com.example.komaexchange.tools
//
//import com.example.helloexchangekotlin.entities.Order
//import io.andrewohara.dynamokt.DataClassTableSchema
//import software.amazon.awssdk.enhanced.dynamodb.TableSchema
//
//fun main() {
//    val tableSchema = TableSchema.builder(Order.Companion::class.java).build()
//    val xx = DataClassTableSchema(Order::class)
//    println(tableSchema)
//}