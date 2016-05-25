/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Converts input rows to one or more XML files.
 *
 * @author Matt
 * @since 14-jan-2006
 */
public class JsonOutput extends BaseStep implements StepInterface {
    private static Class<?> PKG = JsonOutput.class; // for i18n purposes, needed by Translator2!!

    private JsonOutputMeta meta;
    private JsonOutputData data;

    private ObjectNode itemNode;
    private JsonNodeFactory nc;
    private int blocKeyNameIndex;
    private String blocKeyNamePrev;
    private List<ObjectNode> jsonItems;
    private ObjectMapper mapper;

    public JsonOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                      Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public void manageRowItems(Object[] row) throws KettleException {

        String currentBlocKeyNameValue = data.inputRowMeta.getString(row, blocKeyNameIndex);
        if (!blocKeyNamePrev.equals(currentBlocKeyNameValue)) {
            // Otput the new row
            outPutRow(row);
            jsonItems = new ArrayList<>();
        }

        // Create a new object with specified fields
        itemNode = new ObjectNode(nc);

        for (int i = 0; i < data.nrFields; i++) {
            JsonOutputField outputField = meta.getOutputFields()[i];

            ValueMetaInterface v = data.inputRowMeta.getValueMeta(data.fieldIndexes[i]);

            switch (v.getType()) {
                case ValueMetaInterface.TYPE_BOOLEAN:
                    itemNode.put(outputField.getElementName(), data.inputRowMeta.getBoolean(row, data.fieldIndexes[i]));
                    break;
                case ValueMetaInterface.TYPE_INTEGER:
                    itemNode.put(outputField.getElementName(), data.inputRowMeta.getInteger(row, data.fieldIndexes[i]));
                    break;
                case ValueMetaInterface.TYPE_NUMBER:
                    itemNode.put(outputField.getElementName(), data.inputRowMeta.getNumber(row, data.fieldIndexes[i]));
                    break;
                case ValueMetaInterface.TYPE_BIGNUMBER:
                    itemNode.put(outputField.getElementName(), data.inputRowMeta.getBigNumber(row, data.fieldIndexes[i]));
                    break;
                default:
                    String value = data.inputRowMeta.getString(row, data.fieldIndexes[i]);
                    if(outputField.isJSONFragment()) {
                        try {
                            JsonNode jsonNode = mapper.readTree(value);
                            itemNode.put(outputField.getElementName(), jsonNode);
                        } catch (IOException e) {
                            // TBD Exception must be properly managed
                            e.printStackTrace();
                        }
                    } else {
                        itemNode.put(outputField.getElementName(), value);
                    }

                    break;
            }
        }
        jsonItems.add(itemNode);

        data.nrRow++;
        blocKeyNamePrev = currentBlocKeyNameValue;

     /*       if (data.nrRowsInBloc > 0) {
                // System.out.println("data.nrRow%data.nrRowsInBloc = "+ data.nrRow%data.nrRowsInBloc);
                if (data.nrRow % data.nrRowsInBloc == 0) {
                    // We can now output an object
                    // System.out.println("outputting the row.");
                    outPutRow(row);
                }
            }*/
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

        meta = (JsonOutputMeta) smi;
        data = (JsonOutputData) sdi;

        Object[] r = getRow(); // This also waits for a row to be finished.
        if (r == null) {
            outPutRow(r);
            setOutputDone();
            return false;
        }

        if (first) {

            nc = new ObjectMapper().getNodeFactory();
            mapper = new ObjectMapper();
            jsonItems = new ArrayList<>();

            first = false;
            data.inputRowMeta = getInputRowMeta();
            data.inputRowMetaSize = data.inputRowMeta.size();

            blocKeyNameIndex = data.inputRowMeta.indexOfValue(meta.getBlockKeyName());
            blocKeyNamePrev = data.inputRowMeta.getString(r, blocKeyNameIndex);

            ValueMetaInterface keyValueMeta = data.inputRowMeta.getValueMeta(blocKeyNameIndex);

            if (data.outputValue) {
                // Create new structure for output fields
                data.outputRowMeta = new RowMeta();
                data.outputRowMeta.addValueMeta(0, new ValueMetaString(meta.getBlockKeyName()));
                data.outputRowMeta.addValueMeta(1, new ValueMetaString(meta.getOutputValue()));
            }

            // Cache the field name indexes
            //
            data.nrFields = meta.getOutputFields().length;
            data.fieldIndexes = new int[data.nrFields];
            for (int i = 0; i < data.nrFields; i++) {
                data.fieldIndexes[i] = data.inputRowMeta.indexOfValue(meta.getOutputFields()[i].getFieldName());
                if (data.fieldIndexes[i] < 0) {
                    throw new KettleException(BaseMessages.getString(PKG, "JsonOutput.Exception.FieldNotFound"));
                }
                JsonOutputField field = meta.getOutputFields()[i];
                field.setElementName(environmentSubstitute(field.getElementName()));
            }
        }

        manageRowItems(r);

        if (data.writeToFile && !data.outputValue) {
            putRow(data.inputRowMeta, r); // in case we want it go further...
            incrementLinesOutput();
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private void outPutRow(Object[] rowData) throws KettleStepException {
        // We can now output an object
        String value = null;

        try {
            if (jsonItems != null) {
                if (jsonItems.size() > 0) {
                    ObjectNode theNode = new ObjectNode(nc);
                    // TBD Try to understand if this can have a performance impact and do it better...

                    theNode.put(meta.getJsonBloc(), mapper.readTree(mapper.writeValueAsString(jsonItems.size() > 1
                            ? jsonItems : (!meta.isUseArrayWithSingleInstance() ? jsonItems.get(0) : jsonItems))));
                    value = mapper.writeValueAsString(theNode);
                } else {
                    value = mapper.writeValueAsString((jsonItems.size() > 1
                            ? jsonItems : (!meta.isUseArrayWithSingleInstance() ? jsonItems.get(0) : jsonItems)));
                }
            }

        } catch (IOException e) {
            // TBD Exception must be properly managed
            e.printStackTrace();
        }

        String[] keyRow = new String[1];
        keyRow[0] = blocKeyNamePrev;
        if (data.outputValue && data.outputRowMeta != null) {
            Object[] outputRowData = RowDataUtil.addValueData(keyRow, 1, value);
            incrementLinesOutput();
            putRow(data.outputRowMeta, outputRowData);
        }

        if (data.writeToFile) {
            // Open a file
            if (!openNewFile()) {
                throw new KettleStepException(BaseMessages.getString(
                        PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
            }
            // Write data to file
            try {
                data.writer.write(value);
            } catch (Exception e) {
                throw new KettleStepException(BaseMessages.getString(PKG, "JsonOutput.Error.Writing"), e);
            }
            // Close file
            closeFile();
        }
        // Data are safe
    }

    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (JsonOutputMeta) smi;
        data = (JsonOutputData) sdi;
        if (super.init(smi, sdi)) {

            data.writeToFile = (meta.getOperationType() != JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE);
            data.outputValue = (meta.getOperationType() != JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE);

            if (data.outputValue) {
                // We need to have output field name
                if (Const.isEmpty(environmentSubstitute(meta.getOutputValue()))) {
                    logError(BaseMessages.getString(PKG, "JsonOutput.Error.MissingOutputFieldName"));
                    stopAll();
                    setErrors(1);
                    return false;
                }
            }
            if (data.writeToFile) {
                // We need to have output field name
                if (!meta.isServletOutput() && Const.isEmpty(meta.getFileName())) {
                    logError(BaseMessages.getString(PKG, "JsonOutput.Error.MissingTargetFilename"));
                    stopAll();
                    setErrors(1);
                    return false;
                }
                if (!meta.isDoNotOpenNewFileInit()) {
                    if (!openNewFile()) {
                        logError(BaseMessages.getString(PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
                        stopAll();
                        setErrors(1);
                        return false;
                    }
                }

            }
            data.realBlocName = Const.NVL(environmentSubstitute(meta.getJsonBloc()), "");
            data.blocKeyName = Const.NVL(environmentSubstitute(meta.getBlockKeyName()), "");
            data.nrRowsInBloc = Const.toInt(environmentSubstitute(meta.getNrRowsInBloc()), 0);
            return true;
        }

        return false;
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (JsonOutputMeta) smi;
        data = (JsonOutputData) sdi;
        if (data.ja != null) {
            data.ja = null;
        }
        if (data.jg != null) {
            data.jg = null;
        }
        closeFile();
        super.dispose(smi, sdi);

    }

    private void createParentFolder(String filename) throws KettleStepException {
        if (!meta.isCreateParentFolder()) {
            return;
        }
        // Check for parent folder
        FileObject parentfolder = null;
        try {
            // Get parent folder
            parentfolder = KettleVFS.getFileObject(filename, getTransMeta()).getParent();
            if (!parentfolder.exists()) {
                if (log.isDebug()) {
                    logDebug(BaseMessages.getString(PKG, "JsonOutput.Error.ParentFolderNotExist", parentfolder.getName()));
                }
                parentfolder.createFolder();
                if (log.isDebug()) {
                    logDebug(BaseMessages.getString(PKG, "JsonOutput.Log.ParentFolderCreated"));
                }
            }
        } catch (Exception e) {
            throw new KettleStepException(BaseMessages.getString(
                    PKG, "JsonOutput.Error.ErrorCreatingParentFolder", parentfolder.getName()));
        } finally {
            if (parentfolder != null) {
                try {
                    parentfolder.close();
                } catch (Exception ex) { /* Ignore */
                }
            }
        }
    }

    public boolean openNewFile() {
        if (data.writer != null) {
            return true;
        }
        boolean retval = false;

        try {

            if (meta.isServletOutput()) {
                data.writer = getTrans().getServletPrintWriter();
            } else {
                String filename = buildFilename();
                createParentFolder(filename);
                if (meta.AddToResult()) {
                    // Add this to the result file names...
                    ResultFile resultFile =
                            new ResultFile(
                                    ResultFile.FILE_TYPE_GENERAL, KettleVFS.getFileObject(filename, getTransMeta()),
                                    getTransMeta().getName(), getStepname());
                    resultFile.setComment(BaseMessages.getString(PKG, "JsonOutput.ResultFilenames.Comment"));
                    addResultFile(resultFile);
                }

                OutputStream outputStream;
                OutputStream fos = KettleVFS.getOutputStream(filename, getTransMeta(), meta.isFileAppended());
                outputStream = fos;

                if (!Const.isEmpty(meta.getEncoding())) {
                    data.writer =
                            new OutputStreamWriter(new BufferedOutputStream(outputStream, 5000), environmentSubstitute(meta
                                    .getEncoding()));
                } else {
                    data.writer = new OutputStreamWriter(new BufferedOutputStream(outputStream, 5000));
                }

                if (log.isDetailed()) {
                    logDetailed(BaseMessages.getString(PKG, "JsonOutput.FileOpened", filename));
                }

                data.splitnr++;
            }

            retval = true;

        } catch (Exception e) {
            logError(BaseMessages.getString(PKG, "JsonOutput.Error.OpeningFile", e.toString()));
        }

        return retval;
    }

    public String buildFilename() {
        return meta.buildFilename(environmentSubstitute(meta.getFileName()), getCopy(), data.splitnr);
    }

    private boolean closeFile() {
        if (data.writer == null) {
            return true;
        }
        boolean retval = false;

        try {
            data.writer.close();
            data.writer = null;
            retval = true;
        } catch (Exception e) {
            logError(BaseMessages.getString(PKG, "JsonOutput.Error.ClosingFile", e.toString()));
            setErrors(1);
            retval = false;
        }

        return retval;
    }
}
