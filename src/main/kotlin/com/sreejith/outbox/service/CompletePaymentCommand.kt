package com.sreejith.outbox.service

import java.math.BigDecimal

data class CompletePaymentCommand(
	val paymentId: String,
	val amount: BigDecimal,
	val currency: String,
)
