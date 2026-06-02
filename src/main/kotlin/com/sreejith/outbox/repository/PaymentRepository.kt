package com.sreejith.outbox.repository

import com.sreejith.outbox.domain.Payment
import org.springframework.data.jpa.repository.JpaRepository

interface PaymentRepository : JpaRepository<Payment, String>
