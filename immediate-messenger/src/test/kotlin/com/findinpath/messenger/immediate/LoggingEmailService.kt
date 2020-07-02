package com.findinpath.messenger.immediate

import org.slf4j.LoggerFactory

class LoggingEmailService(val callback: (EmailDetails) -> Unit) : EmailService{
    private val logger = LoggerFactory.getLogger(javaClass)


    override fun sendEmail(emailDetails: EmailDetails) {
        logger.info("Handling email details $emailDetails")
        callback(emailDetails)
    }
}