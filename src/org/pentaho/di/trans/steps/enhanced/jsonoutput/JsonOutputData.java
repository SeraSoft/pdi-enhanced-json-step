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

import java.io.Writer;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.BitSet;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * @author Matt
 * @since 22-jan-2005
 */
public class JsonOutputData extends BaseStepData implements StepDataInterface {

    public RowMetaInterface inputRowMeta;
    public RowMetaInterface outputRowMeta;
    public int inputRowMetaSize;

    public int nrFields;
    public int[] fieldIndexes;
    public int[] keysGroupIndexes;
    public int nrRow;

    private boolean outputValue;
    private boolean writeToFile;
    private boolean genFlat;
    private boolean genLoopOverKey;

    public String realBlocName;
    public int splitnr;
    public Writer writer;

    /**
     *
     */
    public JsonOutputData() {
        super();

        this.nrRow = 0;
        this.outputValue = false;
        this.writeToFile = false;
        this.genFlat = false;
        this.genLoopOverKey = false;
        this.writer = null;
    }

    public boolean isGenFlat() {
        return genFlat;
    }

    public void setGenFlat(boolean genFlat) {
        this.genFlat = genFlat;
    }

    public boolean isGenLoopOverKey() {
        return genLoopOverKey;
    }

    public void setGenLoopOverKey(boolean genLoopOverKey) {
        this.genLoopOverKey = genLoopOverKey;
    }

    public boolean isOutputValue() {
        return outputValue;
    }

    public void setOutputValue(boolean outputValue) {
        this.outputValue = outputValue;
    }

    public boolean isWriteToFile() {
        return writeToFile;
    }

    public void setWriteToFile(boolean writeToFile) {
        this.writeToFile = writeToFile;
    }
}
