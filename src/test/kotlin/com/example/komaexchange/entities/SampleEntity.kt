package com.example.komaexchange.entities

import java.math.BigDecimal

data class SampleEntity(
    val id: Long, // ID
    val name: String, // 名前
    val value: BigDecimal, // 値
    val createdAt: Long, // 作成日時
) {
    companion object {
        fun create(
            id: Long,
            name: String = "name!",
            value: BigDecimal = BigDecimal(0),
            createdAt: Long = System.currentTimeMillis()
        ): SampleEntity {
            return SampleEntity(
                id, // ID
                name, // 名前
                value, // 値
                createdAt, // 作成日時
            )
        }
    }

}
