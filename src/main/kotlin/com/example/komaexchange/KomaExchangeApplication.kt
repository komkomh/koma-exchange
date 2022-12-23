package com.example.komaexchange

import com.example.komaexchange.wokrkers.OrderExecuteWorker
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KomaExchangeApplication : ApplicationRunner {
    override fun run(args: ApplicationArguments?) {
        println("hello application runner!")

        val receivers = listOf(
            StreamReceiver(OrderExecuteWorker())
        )

        while (true) {
            receivers.forEach { it.execute() }
            Thread.sleep(5000)
        }
    }
}

fun main(args: Array<String>) {
    runApplication<KomaExchangeApplication>(*args)
}
