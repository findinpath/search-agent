package com.findinpath.messenger.immediate

import com.findinpath.messenger.immediate.EmailDetails

interface EmailService {

    fun sendEmail(emailDetails: EmailDetails)
}