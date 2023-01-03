package com.example.komaexchange.entities

import java.math.BigDecimal

data class SampleEntity(
    val id: Long, // ID
    val name: String, // 名前
    val value: BigDecimal, // 値
    override var sequenceNumber: String?, // 処理ID
    val createdAt: Long, // 作成日時
) : RecordEntity(sequenceNumber){
    companion object {
        fun create(
            id: Long,
            name: String = "name!",
            value: BigDecimal = BigDecimal(0),
            sequenceNumber: String? = null,
            createdAt: Long = System.currentTimeMillis()
        ): SampleEntity {
            return SampleEntity(
                id, // ID
                name, // 名前
                value, // 値
                sequenceNumber, // シーケンスNo
                createdAt, // 作成日時
            )
        }
    }

}
