/*
 * Copyright 2010 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.pig.scala.aggregate

import _root_.st.happy_camper.pig.scala.aggregate.evaluation.LongToDateFormat
import _root_.st.happy_camper.pig.scala.aggregate.storage.CombinedLogLoader

import _root_.org.apache.pig._

object Aggregator {
  def main(args : Array[String]) {
    val execType = args(0)
    val input = args(1)
    val output = args(2)

    val pigServer = new PigServer(execType)
    pigServer.registerQuery("request = " +
    		"LOAD '" + input + "' USING " + classOf[CombinedLogLoader].getName + ";")
    pigServer.registerQuery("html_request = " +
    		"FILTER request " +
    		"BY method == 'GET' AND request_path matches '(?:/[^ ]*)?/(?:[^/]+\\\\.html)?' AND (status_code == 200 OR status_code == 304);")
    pigServer.registerQuery("fixed_html_request = " +
    		"FOREACH html_request " +
    		"GENERATE " +
    		"  remote_host, " +
    		"  (request_path matches '.*/$' ? CONCAT(request_path, 'index.html') : request_path) AS request_path, " +
    		"  " + classOf[LongToDateFormat].getName + "(requested_time, 'yyyy/MM/dd') AS request_date;")
    pigServer.registerQuery("grouped_requests = " +
    		"GROUP fixed_html_request " +
    		"BY (remote_host, request_path, request_date);")
    pigServer.registerQuery("count = " +
    		"FOREACH grouped_requests " +
    		"GENERATE FLATTEN(group), COUNT(fixed_html_request);")
    pigServer.store("count", output)
  }
}
