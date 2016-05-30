/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.enhanced.jsonoutput;

import org.pentaho.di.core.injection.Injection;

/**
 * Describes a single field in an Json output file
 *
 * @author Samatar
 * @since 14-june-2010
 */
public class JsonOutputKeyField implements Cloneable {

    @Injection( name = "JSON_FIELDNAME", group = "KEY_FIELDS" )
    private String fieldName;

    public JsonOutputKeyField(String fieldName) {
        this.fieldName = fieldName;
    }

    public JsonOutputKeyField() {
    }

    public int compare(Object obj) {
        JsonOutputKeyField field = (JsonOutputKeyField) obj;

        return fieldName.compareTo(field.getFieldName());
    }

    public boolean equal(Object obj) {
        JsonOutputKeyField field = (JsonOutputKeyField) obj;

        return fieldName.equals(field.getFieldName());
    }

    public Object clone() {
        try {
            Object retval = super.clone();
            return retval;
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldname) {
        this.fieldName = fieldname;
    }

}
