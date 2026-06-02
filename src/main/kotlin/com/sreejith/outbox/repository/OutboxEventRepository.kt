package com.sreejith.outbox.repository

import com.sreejith.outbox.domain.OutboxEvent
import org.springframework.data.jpa.repository.JpaRepository

interface OutboxEventRepository : JpaRepository<OutboxEvent, String>
