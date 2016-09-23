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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
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
 * Converts input rows to one or more JSON structures.
 *
 * @author Sergio Ramazzina
 * @since 14-jan-2006
 */
public class JsonOutput extends BaseStep implements StepInterface {
    private static Class<?> PKG = JsonOutput.class; // for i18n purposes, needed by Translator2!!

    private JsonOutputMeta meta;
    private JsonOutputData data;
    public  Object[] prevRow;

    private ObjectNode itemNode;
    private JsonNodeFactory nc;
    private List<ObjectNode> jsonItems;
    private ObjectMapper mapper;
    private int startPagePos;

    public JsonOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                      Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public void manageRowItems(Object[] row) throws KettleException {

        if (data.isGenLoopOverKey() && !sameGroup( prevRow, row ) && jsonItems.size()>0) {
            // Output the new row
            logDebug("Record Num: " + data.nrRow + " - Generating JSON chunk");
            outPutRow(prevRow);
            jsonItems = new ArrayList<>();
        }

        // Create a new object with specified fields
        itemNode = new ObjectNode(nc);

        for (int i = 0; i < data.nrFields; i++) {
            JsonOutputField outputField = meta.getOutputFields()[i];

            ValueMetaInterface v = data.inputRowMeta.getValueMeta(data.fieldIndexes[i]);

            switch (v.getType()) {
                case ValueMetaInterface.TYPE_BOOLEAN:
                    Boolean boolValue = data.inputRowMeta.getBoolean(row, data.fieldIndexes[i]);

                    if (boolValue != null)
                        itemNode.put(outputField.getElementName(), boolValue);
                    else {
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), boolValue);
                    }
                    break;

                case ValueMetaInterface.TYPE_INTEGER:
                    Long integerValue = data.inputRowMeta.getInteger(row, data.fieldIndexes[i]);

                    if (integerValue != null)
                        itemNode.put(outputField.getElementName(), integerValue);
                    else
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), integerValue);
                    break;
                case ValueMetaInterface.TYPE_NUMBER:
                    Double numberValue = data.inputRowMeta.getNumber(row, data.fieldIndexes[i]);

                    if (numberValue != null)
                        itemNode.put(outputField.getElementName(), numberValue);
                    else
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), numberValue);
                    break;
                case ValueMetaInterface.TYPE_BIGNUMBER:
                    BigDecimal bignumberValue = data.inputRowMeta.getBigNumber(row, data.fieldIndexes[i]);

                    if(bignumberValue != null)
                        itemNode.put(outputField.getElementName(), bignumberValue);
                    else
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), bignumberValue);
                    break;
                default:
                    String value = data.inputRowMeta.getString(row, data.fieldIndexes[i]);
                    if (value != null) {
                        if (outputField.isJSONFragment()) {
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
                    } else {
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), value);
                    }

                    break;
            }
        }

        jsonItems.add(itemNode);
        prevRow = data.inputRowMeta.cloneRow( row ); // copy the row to previous
        data.nrRow++;

        if (meta.getSplitOutputAfter() > 0 && (data.nrRow) % meta.getSplitOutputAfter() == 0) {
            // Output the new row
            logDebug("Record Num: " + data.nrRow + " - Generating JSON chunk");
            outPutRow(prevRow);
            jsonItems = new ArrayList<>();
        }

    }

    // Is the row r of the same group as previous?
    private boolean sameGroup( Object[] previous, Object[] r ) throws KettleValueException {
        return data.inputRowMeta.compare( previous, r, data.keysGroupIndexes ) == 0;
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

        meta = (JsonOutputMeta) smi;
        data = (JsonOutputData) sdi;

        Object[] r = getRow(); // This also waits for a row to be finished.
        if (r == null) {
            outPutRow(prevRow);
            setOutputDone();
            return false;
        }

        if (first) {

            if (onFirstRecord(r)) return false;

        }

        manageRowItems(r);

        if (data.isWriteToFile() && !data.isOutputValue()) {
            putRow(data.inputRowMeta, r); // in case we want it go further...
            incrementLinesOutput();
        }
        return true;
    }

    private boolean onFirstRecord(Object[] r) throws KettleException {

        nc = new ObjectMapper().getNodeFactory();
        mapper = new ObjectMapper();
        jsonItems = new ArrayList<>();
        startPagePos = 1;

        first = false;
        data.inputRowMeta = getInputRowMeta();
        data.inputRowMetaSize = data.inputRowMeta.size();

        // Init previous row copy to this first row
        prevRow = data.inputRowMeta.cloneRow( r ); // copy the row to previous

        if (data.isOutputValue()) {
            // Create new structure for output fields
            data.outputRowMeta = new RowMeta();
            JsonOutputKeyField[] keyFields = meta.getKeyFields();
            for (int i=0; i<meta.getKeyFields().length; i++) {
                ValueMetaInterface vmi = data.inputRowMeta.getValueMeta(data.inputRowMeta.indexOfValue(keyFields[i].getFieldName()));
                data.outputRowMeta.addValueMeta(i, vmi);
            }

            // This is JSON block's column
            data.outputRowMeta.addValueMeta(meta.getKeyFields().length, new ValueMetaString(meta.getOutputValue()));
        }


        int fieldLength = meta.getKeyFields().length + 1;
        if (meta.getJsonSizeFieldname() != null && meta.getJsonSizeFieldname().length()>0) {
            data.outputRowMeta.addValueMeta(fieldLength, new ValueMetaInteger(meta.getJsonSizeFieldname()));
            fieldLength++;
        }
        if (meta.getJsonPageStartsAtFieldname() != null && meta.getJsonPageStartsAtFieldname().length()>0) {
            data.outputRowMeta.addValueMeta(fieldLength, new ValueMetaInteger(meta.getJsonPageStartsAtFieldname()));
            fieldLength++;
        }
        if (meta.getJsonPageEndsAtFieldname() != null && meta.getJsonPageEndsAtFieldname().length()>0) {
            data.outputRowMeta.addValueMeta(fieldLength, new ValueMetaInteger(meta.getJsonPageEndsAtFieldname()));
            fieldLength++;
        }

        initDataFieldsPositionsArray();


        if (initKeyFieldsPositionArray(r)) return true;
        return false;
    }

    private void initDataFieldsPositionsArray() throws KettleException {
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

    private boolean initKeyFieldsPositionArray(Object[] r) {
        data.keysGroupIndexes = new int[ meta.getKeyFields().length ];

        for ( int i = 0; i < meta.getKeyFields().length; i++ ) {
            data.keysGroupIndexes[ i ] = data.inputRowMeta.indexOfValue( meta.getKeyFields()[ i ].getFieldName() );
            if ( ( r != null ) && ( data.keysGroupIndexes[ i ] < 0 ) ) {
                /* logError( BaseMessages.getString( PKG, "GroupBy.Log.GroupFieldCouldNotFound", meta.getGroupField()[ i ] ) );*/
                setErrors( 1 );
                stopAll();
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private void outPutRow(Object[] rowData) throws KettleStepException {
        // We can now output an object
        String value = null;

        if (jsonItems == null || jsonItems.size()==0)
            return;

        try {
            if (jsonItems != null && jsonItems.size()>0) {
                if (meta.getJsonBloc() != null && meta.getJsonBloc().length()>0) {
                    ObjectNode theNode = new ObjectNode(nc);
                    // TBD Try to understand if this can have a performance impact and do it better...

                    theNode.put(meta.getJsonBloc(), mapper.readTree(mapper.writeValueAsString(jsonItems.size() > 1
                            ? jsonItems : (!meta.isUseArrayWithSingleInstance() ? jsonItems.get(0) : jsonItems))));
                    if (meta.isJsonPrittified())
                        value = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(theNode);
                    else
                        value = mapper.writeValueAsString(theNode);
                } else {
                    if (meta.isJsonPrittified())
                        value = mapper.writerWithDefaultPrettyPrinter().writeValueAsString((jsonItems.size() > 1
                                ? jsonItems : (!meta.isUseArrayWithSingleInstance() ? jsonItems.get(0) : jsonItems)));
                    else
                        value = mapper.writeValueAsString((jsonItems.size() > 1
                            ? jsonItems : (!meta.isUseArrayWithSingleInstance() ? jsonItems.get(0) : jsonItems)));
                }
            }

        } catch (IOException e) {
            // TBD Exception must be properly managed
            e.printStackTrace();
        }

        int jsonLength = value.length();

        if (data.isOutputValue() && data.outputRowMeta != null) {

            Object[] keyRow = new Object[meta.getKeyFields().length];

            for (int i=0; i<meta.getKeyFields().length; i++) {
                try {
                    ValueMetaInterface vmi = data.inputRowMeta.getValueMeta(data.keysGroupIndexes[ i ]);
                    switch (vmi.getType()) {
                        case ValueMetaInterface.TYPE_BOOLEAN:
                            keyRow[i] = data.inputRowMeta.getBoolean(rowData, data.keysGroupIndexes[ i ]);
                            break;
                        case ValueMetaInterface.TYPE_INTEGER:
                            keyRow[i] = data.inputRowMeta.getInteger(rowData, data.keysGroupIndexes[ i ]);
                            break;
                        case ValueMetaInterface.TYPE_NUMBER:
                            keyRow[i] = data.inputRowMeta.getNumber(rowData, data.keysGroupIndexes[ i ]);
                            break;
                        case ValueMetaInterface.TYPE_BIGNUMBER:
                            keyRow[i] = data.inputRowMeta.getBigNumber(rowData, data.keysGroupIndexes[ i ]);
                            break;
                        default:
                            keyRow[i] = data.inputRowMeta.getString(rowData, data.keysGroupIndexes[ i ]);
                            break;
                    }
                } catch (KettleValueException e) {
                    // TODO - Properly handle the exception
                    // e.printStackTrace();
                }
            }

            Object[] fieldsQueueArray = new Object[4];

            fieldsQueueArray[0] = value;
            int nextFieldPos = 1;


            if (meta.getJsonSizeFieldname() != null && meta.getJsonSizeFieldname().length()>0) {
                fieldsQueueArray[nextFieldPos] = new Long(jsonLength);
                nextFieldPos++;
            }
            if (meta.getJsonPageStartsAtFieldname() != null && meta.getJsonPageStartsAtFieldname().length()>0) {
                fieldsQueueArray[nextFieldPos] = new Long(startPagePos);
                startPagePos = data.nrRow + 1;
                nextFieldPos++;
            }
            if (meta.getJsonPageEndsAtFieldname() != null && meta.getJsonPageEndsAtFieldname().length()>0) {
                fieldsQueueArray[nextFieldPos] = new Long(data.nrRow);
                nextFieldPos++;
            }

            Object[] outputRowData = RowDataUtil.addRowData(keyRow, keyRow.length, fieldsQueueArray);
            incrementLinesOutput();

            putRow(data.outputRowMeta, outputRowData);
        }

        if (data.isWriteToFile()) {
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

            data.setWriteToFile((meta.getOperationType() != JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE));
            data.setOutputValue((meta.getOperationType() != JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE));
            data.setGenFlat((meta.getGenerationType() != JsonOutputMeta.GENERATON_TYPE_FLAT));
            data.setGenLoopOverKey((meta.getOperationType() != JsonOutputMeta.GENERATON_TYPE_LOOP_OVER_KEY));

            if (data.isOutputValue()) {
                // We need to have output field name
                if (Const.isEmpty(environmentSubstitute(meta.getOutputValue()))) {
                    logError(BaseMessages.getString(PKG, "JsonOutput.Error.MissingOutputFieldName"));
                    stopAll();
                    setErrors(1);
                    return false;
                }
            }
            if (data.isWriteToFile()) {
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
            return true;
        }

        return false;
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (JsonOutputMeta) smi;
        data = (JsonOutputData) sdi;

        if (jsonItems != null) {
            jsonItems = null;
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
