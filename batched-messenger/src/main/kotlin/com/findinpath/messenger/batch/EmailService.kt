package com.findinpath.messenger.batch

interface EmailService {

    fun sendEmail(emailDetails: EmailDetails)
}