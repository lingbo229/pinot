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
package com.linkedin.pinot.common.restlet.swagger;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * An example of a value, used to document POJOs.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Example {
  /**
   * The contents of the example, usually a string containing JSON.
   */
  String value();
}
