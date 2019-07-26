package com.pixipanda.producer

import java.util.Date

case class AdImpression(id: Int , timestamp: Date, publisher: String, advertiser: String, website: String, geo: String, bid: Double, cookie: String)

case class AdClick(id: Int , timestamp: Date)

