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
package st.happy_camper.pig.scala.aggregate.evaluation

import _root_.java.util._
import _root_.java.text._

import _root_.org.apache.pig._
import _root_.org.apache.pig.data._

/**
 * @author ueshin
 */
class LongToDateFormat extends EvalFunc[String] {

  def exec(input: Tuple) = {
    if(input == null || input.size < 2) {
      null
    }
    else {
      val date = new Date(input.get(0).asInstanceOf[Long])
      val format = input.get(1).asInstanceOf[String]
      new SimpleDateFormat(format).format(date)
    }
  }

}
