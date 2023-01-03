package com.example.komaexchange

import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KomaExchangeApplication : ApplicationRunner {
    override fun run(args: ApplicationArguments?) {
        println("hello application runner!")

        val streamReceiver = StreamReceiver()
        streamReceiver.init()

        while (true) {
            streamReceiver.execute()
            Thread.sleep(10_000)
        }
    }
}

fun main(args: Array<String>) {
    runApplication<KomaExchangeApplication>(*args)
}
