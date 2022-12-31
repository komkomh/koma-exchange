package com.example.komaexchange.workers

import com.example.komaexchange.entities.*
import com.example.komaexchange.utils.MutexQueue
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class MutexQueueTest {
    // テスト対象
    var queue = MutexQueue<SampleEntity>()

    @BeforeEach
    fun init() {
        queue = MutexQueue<SampleEntity>()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @DisplayName("空で取得")
    fun empty1() = runTest {
        // 実行
        val peek1 = queue.peek()

        // 確認
        Assertions.assertThat(peek1).`as`("投入していないのでnullが取得される").isNull()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @DisplayName("1件取得")
    fun peek1() = runTest {
        // 準備
        val entity1 = queue.offer(SampleEntity.create(1L))

        // 実行
        val peek1 = queue.peek()
        val peek2 = queue.peek()

        // 確認
        Assertions.assertThat(peek1).`as`("1件取得される").isEqualTo(entity1)
        Assertions.assertThat(peek2).`as`("投入されていないのでnull").isNull()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @DisplayName("2件取得")
    fun peek2() = runTest {
        // 準備
        val entity1 = queue.offer(SampleEntity.create(1L))
        val entity2 = queue.offer(SampleEntity.create(2L))

        // 実行
        val peek1 = queue.peek()
        val peek2 = queue.peek()
        val peek3 = queue.peek()

        // 確認
        Assertions.assertThat(peek1).`as`("1件取得される").isEqualTo(entity1)
        Assertions.assertThat(peek2).`as`("1件取得される").isEqualTo(entity2)
        Assertions.assertThat(peek3).`as`("投入されていないのでnull").isNull()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @DisplayName("2件取得")
    fun peekWait1() = runTest {
        // 準備
        val entity1 = queue.offer(SampleEntity.create(1L))
        val entity2 = queue.offer(SampleEntity.create(2L))

        // 実行
        val peek1 = queue.peekWait()
        val peek2 = queue.peekWait()

        // 確認
        Assertions.assertThat(peek1).`as`("1件取得される").isEqualTo(entity1)
        Assertions.assertThat(peek2).`as`("1件取得される").isEqualTo(entity2)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @DisplayName("2件取得後にリセット")
    fun reset1() = runTest {
        // 準備
        val entity1 = queue.offer(SampleEntity.create(1L))
        val entity2 = queue.offer(SampleEntity.create(2L))

        // 実行
        val peek1 = queue.peek()
        val peek2 = queue.peek()
        val peek3 = queue.peek()
        queue.reset()
        val peek4 = queue.peek()

        // 確認
        Assertions.assertThat(peek4).`as`("1件取得される").isEqualTo(entity1)
        Assertions.assertThat(queue.size()).`as`("2件投入済み").isEqualTo(2)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @DisplayName("2件取得後にコミット")
    fun done1() = runTest {
        // 準備
        val entity1 = queue.offer(SampleEntity.create(1L))
        val entity2 = queue.offer(SampleEntity.create(2L))

        // 実行
        val peek1 = queue.peek()
        val peek2 = queue.peek()
        val peek3 = queue.peek()
        queue.done()
        val peek4 = queue.peek()

        // 確認
        Assertions.assertThat(peek1).`as`("1件取得される").isEqualTo(entity1)
        Assertions.assertThat(peek2).`as`("1件取得される").isEqualTo(entity2)
        Assertions.assertThat(peek3).`as`("投入されていないのでnull").isNull()
        Assertions.assertThat(peek4).`as`("投入されていないのでnull").isNull()
        Assertions.assertThat(queue.size()).`as`("確定されたので0").isEqualTo(0)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    @DisplayName("2件取得後に1件前まで確定されて1件残")
    fun untilDone1() = runTest {
        // 準備
        val entity1 = queue.offer(SampleEntity.create(1L))
        val entity2 = queue.offer(SampleEntity.create(2L))

        // 実行
        val peek1 = queue.peek()
        val peek2 = queue.peek()
        val peek3 = queue.peek()
        queue.untilDone()
        val peek4 = queue.peek()

        // 確認
        Assertions.assertThat(peek1).`as`("1件取得される").isEqualTo(entity1)
        Assertions.assertThat(peek2).`as`("1件取得される").isEqualTo(entity2)
        Assertions.assertThat(peek3).`as`("投入されていないのでnull").isNull()
        Assertions.assertThat(peek4).`as`("戻されたので1件取得される").isEqualTo(entity2)
        Assertions.assertThat(queue.size()).`as`("片方戻されたので1").isEqualTo(1)
    }
}