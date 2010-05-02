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
package st.happy_camper.pig.scala.aggregate.storage

import _root_.java.nio.charset.Charset
import _root_.java.text._
import _root_.java.util._

import _root_.org.apache.pig._
import _root_.org.apache.pig.backend.datastorage.DataStorage
import _root_.org.apache.pig.builtin.Utf8StorageConverter
import _root_.org.apache.pig.impl.io.BufferedPositionedInputStream
import _root_.org.apache.pig.impl.logicalLayer.schema.Schema
import _root_.org.apache.pig.data._

/**
 * @author ueshin
 */
class CombinedLogLoader extends Utf8StorageConverter with LoadFunc with ApacheLogLoader {

  val COMBINED_PATTERN = "^([^ ]+) - ([^ ]+) \\[([^]]+)\\] \"([A-Z]+)\\s+(.+)\\s+([^ \"]+)\\s*\" ([0-9]+) ([0-9]+|-) \"([^\"]*)\" \"((?:\\\\\"|[^\"])*)\"$".r

  def parseLog = {
    case COMBINED_PATTERN(
      RemoteHost(remoteHost),
      RemoteUser(remoteUser),
      RequestedTime(requestedTime),
      Method(method),
      RequestPath(requestPath),
      Protocol(protocol),
      StatusCode(statusCode),
      ContentLength(contentLength),
      Referer(referer),
      UserAgent(userAgent)
    ) => {
      val tuple = tupleFactory.newTuple(10)
      tuple.set(0, remoteHost)
      remoteUser map { u => tuple.set(1, u) }
      requestedTime map { t => tuple.set(2, t.getTime) }
      tuple.set(3, method)
      tuple.set(4, requestPath)
      tuple.set(5, protocol)
      tuple.set(6, statusCode)
      contentLength map { l => tuple.set(7, l) }
      referer map { r => tuple.set(8, r) }
      userAgent map { u => tuple.set(9, u) }
      tuple
    }
  }

  override def determineSchema(fileName: String, execType: ExecType, storage: DataStorage): Schema = {
    val schema = new Schema
    schema.add(new Schema.FieldSchema("remote_host",    DataType.CHARARRAY))
    schema.add(new Schema.FieldSchema("remote_user",    DataType.CHARARRAY))
    schema.add(new Schema.FieldSchema("requested_time", DataType.LONG))
    schema.add(new Schema.FieldSchema("method",         DataType.CHARARRAY))
    schema.add(new Schema.FieldSchema("request_path",   DataType.CHARARRAY))
    schema.add(new Schema.FieldSchema("protocol",       DataType.CHARARRAY))
    schema.add(new Schema.FieldSchema("status_code",    DataType.INTEGER))
    schema.add(new Schema.FieldSchema("content_length", DataType.LONG))
    schema.add(new Schema.FieldSchema("referer",        DataType.CHARARRAY))
    schema.add(new Schema.FieldSchema("user_agent",     DataType.CHARARRAY))
    schema
  }

}
