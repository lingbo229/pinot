/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.scan.query;

import java.util.ArrayList;
import java.util.Collections;

import com.linkedin.pinot.core.query.utils.Pair;

public class MinMaxRangeFunction extends AggregationFunc {
  private static final String _name = "minmaxrange";

  MinMaxRangeFunction(ResultTable rows, String column) {
    super(rows, column);
  }

  @Override
  public ResultTable run() {
    Double max = Double.NEGATIVE_INFINITY;
    Double min = Double.POSITIVE_INFINITY;

    for (ResultTable.Row row : _rows) {
      max = Math.max(max, new Double(row.get(_column, _name).toString()));
      min = Math.min(min, new Double(row.get(_column, _name).toString()));
    }

    ResultTable resultTable = new ResultTable(new ArrayList<Pair>(), 1);
    resultTable.add(0, max - min);

    return resultTable;
  }
}
