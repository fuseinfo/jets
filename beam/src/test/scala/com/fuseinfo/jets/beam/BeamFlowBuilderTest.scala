/*
 * Copyright (c) 2019 Fuseinfo Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.fuseinfo.jets.beam

import com.fuseinfo.jets.beam.source.JsonData
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.FunSuite
import redis.embedded.RedisServer

class BeamFlowBuilderTest extends FunSuite with EmbeddedKafka {
  test("test beam flow builder") {
    implicit val config: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(customBrokerProperties=Map("zookeeper.connection.timeout.ms"->"60000"))
    withRunningKafka {
      EmbeddedKafka.createCustomTopic("events-unit-json")
      EmbeddedKafka.createCustomTopic("events-unit-avro")
      EmbeddedKafka.createCustomTopic("alert-json")
      val redisServer = new RedisServer(6079)
      redisServer.start()
      try {
        JsonData.loadRealTimeData("events-unit-json", "events.txt")
        new Thread() {
          override def run(): Unit = {
            BeamFlowBuilder.main(Array("--blockOnRun=false"))
          }
        }.start()

        val resultsReg = JsonData.readData("alert-json", 2, 60000).toSet.toSeq.sorted
        assert(resultsReg.size === 2)
        assert(resultsReg(0) endsWith """800, "alert_type": "updateProfileAlert", "customer_id": 501011192685353, "customer_name": "Eva Bush", "old_street_address": "10 YONGE ST", "old_additional_info": "SUITE 3847", "old_city": "TORONTO", "old_province": "ON", "old_postal_code": "M1N2P3", "old_country": "CANADA", "new_street_address": "100 BLOOR ST", "new_additional_info": "APT 2802", "new_city": "TORONTO", "new_province": "ON", "new_postal_code": "M5P1N3", "new_country": "CANADA", "ip_address": "172.20.76.129"}""")
        assert(resultsReg(1) endsWith """900, "alert_type": "updateProfileAlert", "customer_id": 501013368151451, "customer_name": "David Jone", "old_street_address": "100 BLOOR ST", "old_additional_info": "APT 1510", "old_city": "TORONTO", "old_province": "ON", "old_postal_code": "M5P1N3", "old_country": "CANADA", "new_street_address": "33 BAY ST", "new_additional_info": "SUITE 2849", "new_city": "TORONTO", "new_province": "ON", "new_postal_code": "M5N2Z4", "new_country": "CANADA", "ip_address": "172.16.38.33"}""")

        JsonData.loadRealTimeData("events-unit-json", "events_ooo.txt")
        val resultsOOO = JsonData.readData("alert-json", 2, 60000).toSet.toSeq.sorted
        assert(resultsOOO.size === 2)
        assert(resultsOOO(1) endsWith """900, "alert_type": "updateProfileAlert", "customer_id": 601013368151451, "customer_name": "David Jone", "old_street_address": "100 BLOOR ST", "old_additional_info": "APT 1510", "old_city": "TORONTO", "old_province": "ON", "old_postal_code": "M5P1N3", "old_country": "CANADA", "new_street_address": "33 BAY ST", "new_additional_info": "SUITE 2849", "new_city": "TORONTO", "new_province": "ON", "new_postal_code": "M5N2Z4", "new_country": "CANADA", "ip_address": "172.16.38.33"}""")
        assert(resultsOOO(0) endsWith """800, "alert_type": "updateProfileAlert", "customer_id": 601011192685353, "customer_name": "Eva Bush", "old_street_address": "10 YONGE ST", "old_additional_info": "SUITE 3847", "old_city": "TORONTO", "old_province": "ON", "old_postal_code": "M1N2P3", "old_country": "CANADA", "new_street_address": "100 BLOOR ST", "new_additional_info": "APT 2802", "new_city": "TORONTO", "new_province": "ON", "new_postal_code": "M5P1N3", "new_country": "CANADA", "ip_address": "172.20.76.129"}""")

        JsonData.loadRealTimeData("events-unit-json", "events_missing.txt")
        val resultsMissing = JsonData.readData("alert-json", 2, 60000).toSet.toSeq.sorted
        assert(resultsMissing.size === 2)
        assert(resultsMissing(1) endsWith """900, "alert_type": "updateProfileAlert", "customer_id": 701013368151451, "customer_name": "David Jone", "old_street_address": "100 BLOOR ST", "old_additional_info": "APT 1510", "old_city": "TORONTO", "old_province": "ON", "old_postal_code": "M5P1N3", "old_country": "CANADA", "new_street_address": "33 BAY ST", "new_additional_info": "SUITE 2849", "new_city": "TORONTO", "new_province": "ON", "new_postal_code": "M5N2Z4", "new_country": "CANADA", "ip_address": "172.16.38.33"}""")
        assert(resultsMissing(0) endsWith """800, "alert_type": "updateProfileAlert", "customer_id": 701011192685353, "customer_name": "Eva Bush", "old_street_address": "", "old_additional_info": "", "old_city": "", "old_province": "", "old_postal_code": "", "old_country": "", "new_street_address": "100 BLOOR ST", "new_additional_info": "APT 2802", "new_city": "TORONTO", "new_province": "ON", "new_postal_code": "M5P1N3", "new_country": "CANADA", "ip_address": "172.20.76.129"}""")
      } finally {
        redisServer.stop()
      }
    }
  }
}
