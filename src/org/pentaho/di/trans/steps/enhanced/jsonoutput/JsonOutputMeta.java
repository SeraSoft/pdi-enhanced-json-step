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

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * This class knows how to handle the MetaData for the Json output step
 *
 * @since 14-june-2010
 */
@Step(id = "EnhancedJsonOutput",
        image = "JSO.svg",
        i18nPackageName = "org.pentaho.di.trans.steps.enhanced.jsonoutput",
        name = "EnhancedJsonOutput.name",
        description = "EnhancedJsonOutput.description",
        categoryDescription = "EnhancedJsonOutput.category")
@InjectionSupported(localizationPrefix = "JsonOutput.Injection.", groups = {"GENERAL", "KEY_FIELDS", "FIELDS"})
public class JsonOutputMeta extends BaseStepMeta implements StepMetaInterface {
    private static Class<?> PKG = JsonOutputMeta.class; // for i18n purposes, needed by Translator2!!

    /**
     * Operations type
     */
    @Injection(name = "OPERATION", group = "GENERAL")
    private int operationType;

    /**
     * The operations description
     */
    public static final String[] operationTypeDesc = {
            BaseMessages.getString(PKG, "JsonOutputMeta.operationType.OutputValue"),
            BaseMessages.getString(PKG, "JsonOutputMeta.operationType.WriteToFile"),
            BaseMessages.getString(PKG, "JsonOutputMeta.operationType.Both")};

    /**
     * The operations type codes
     */
    public static final String[] operationTypeCode = {"outputvalue", "writetofile", "both"};

    public static final int OPERATION_TYPE_OUTPUT_VALUE = 0;

    public static final int OPERATION_TYPE_WRITE_TO_FILE = 1;

    public static final int OPERATION_TYPE_BOTH = 2;

    public static final int GENERATON_TYPE_FLAT = 0;
    public static final int GENERATON_TYPE_LOOP_OVER_KEY = 1;

    /**
     * The generation type description
     */
    public static final String[] generationTypeDesc = {
            BaseMessages.getString(PKG, "JsonOutputMeta.generationType.Flat"),
            BaseMessages.getString(PKG, "JsonOutputMeta.generationType.LoopOverKey")};

    /**
     * Generations type
     */
    @Injection(name = "GENERATION", group = "GENERAL")
    private int generationType;

    /**
     * The generations type codes
     */
    public static final String[] generationTypeCode = {"flat", "loopOverKey"};

    /**
     * The encoding to use for reading: null or empty string means system default encoding
     */
    @Injection(name = "ENCODING", group = "GENERAL")
    private String encoding;

    /**
     * The name value containing the resulting Json fragment
     */
    @Injection(name = "OUTPUT_VALUE", group = "GENERAL")
    private String outputValue;

    /**
     * The name of the json bloc
     */
    @Injection(name = "JSON_BLOC_NAME", group = "GENERAL")
    private String jsonBloc;

    /**
     * Choose if you want the output prittyfied
     */
    @Injection(name = "PRITTIFY", group = "GENERAL")
    private boolean jsonPrittified;

    /**
     * Choose if you want the output prittyfied
     */
    @Injection(name = "SPLIT_OUTPUT_AFTER", group = "GENERAL")
    private int splitOutputAfter;


  /* THE FIELD SPECIFICATIONS ... */

    /**
     * The output fields
     */
    @InjectionDeep
    private JsonOutputField[] outputFields;

    /**
     * The key fields
     */
    @InjectionDeep
    private JsonOutputKeyField[] keyFields;

    @Injection(name = "ADD_TO_RESULT", group = "GENERAL")
    private boolean addToResult;

    /**
     * Whether to push the output into the output of a servlet with the executeTrans Carte/DI-Server servlet
     */
    @Injection(name = "PASS_TO_SERVLET", group = "GENERAL")
    private boolean servletOutput;

    /**
     * The base name of the output file
     */
    @Injection(name = "FILE_NAME", group = "GENERAL")
    private String fileName;

    /**
     * The file extention in case of a generated filename
     */
    @Injection(name = "EXTENSION", group = "GENERAL")
    private String extension;

    /**
     * Flag to indicate the we want to append to the end of an existing file (if it exists)
     */
    @Injection(name = "APPEND", group = "GENERAL")
    private boolean fileAppended;

    /**
     * Flag to indicate to force unmarshall to JSON Arrays even with a single occurrence in a list
     */
    @Injection(name = "FORCE_JSON_ARRAYS", group = "GENERAL")
    private boolean useArrayWithSingleInstance;

    /**
     * Flag: add the stepnr in the filename
     */
    private boolean stepNrInFilename;

    /**
     * Flag: add the partition number in the filename
     */
    private boolean partNrInFilename;

    /**
     * Flag: add the date in the filename
     */
    private boolean dateInFilename;

    /**
     * Flag: add the time in the filename
     */
    private boolean timeInFilename;

    /**
     * Flag: create parent folder if needed
     */
    @Injection(name = "CREATE_PARENT_FOLDER", group = "GENERAL")
    private boolean createparentfolder;

    private boolean doNotOpenNewFileInit;


    private String jsonSizeFieldname;
    private String jsonPageStartsAtFieldname;
    private String jsonPageEndsAtFieldname;


    public String getJsonSizeFieldname() {
        return jsonSizeFieldname;
    }

    public void setJsonSizeFieldname(String jsonSizeFieldname) {
        this.jsonSizeFieldname = jsonSizeFieldname;
    }

    public String getJsonPageStartsAtFieldname() {
        return jsonPageStartsAtFieldname;
    }

    public void setJsonPageStartsAtFieldname(String jsonPageStartsAtFieldname) {
        this.jsonPageStartsAtFieldname = jsonPageStartsAtFieldname;
    }

    public String getJsonPageEndsAtFieldname() {
        return jsonPageEndsAtFieldname;
    }

    public void setJsonPageEndsAtFieldname(String jsonPageEndsAtFieldname) {
        this.jsonPageEndsAtFieldname = jsonPageEndsAtFieldname;
    }

    public JsonOutputMeta() {
        super(); // allocate BaseStepMeta
    }

    public boolean isDoNotOpenNewFileInit() {
        return doNotOpenNewFileInit;
    }

    public void setDoNotOpenNewFileInit(boolean DoNotOpenNewFileInit) {
        this.doNotOpenNewFileInit = DoNotOpenNewFileInit;
    }

    /**
     * @return Returns the create parent folder flag.
     */
    public boolean isCreateParentFolder() {
        return createparentfolder;
    }

    /**
     * @param createparentfolder The create parent folder flag to set.
     */
    public void setCreateParentFolder(boolean createparentfolder) {
        this.createparentfolder = createparentfolder;
    }

    /**
     * @return Returns the extension.
     */
    public String getExtension() {
        return extension;
    }

    /**
     * @param extension The extension to set.
     */
    public void setExtension(String extension) {
        this.extension = extension;
    }

    /**
     * @return Returns the fileAppended.
     */
    public boolean isFileAppended() {
        return fileAppended;
    }

    /**
     * @param fileAppended The fileAppended to set.
     */
    public void setFileAppended(boolean fileAppended) {
        this.fileAppended = fileAppended;
    }

    /**
     * @return Returns the fileName.
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * @return Returns the timeInFilename.
     */
    public boolean isTimeInFilename() {
        return timeInFilename;
    }

    /**
     * @return Returns the dateInFilename.
     */
    public boolean isDateInFilename() {
        return dateInFilename;
    }

    /**
     * @param dateInFilename The dateInFilename to set.
     */
    public void setDateInFilename(boolean dateInFilename) {
        this.dateInFilename = dateInFilename;
    }

    /**
     * @param timeInFilename The timeInFilename to set.
     */
    public void setTimeInFilename(boolean timeInFilename) {
        this.timeInFilename = timeInFilename;
    }

    /**
     * @param fileName The fileName to set.
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * @return Returns the Add to result filename flag.
     */
    public boolean AddToResult() {
        return addToResult;
    }

    public int getOperationType() {
        return operationType;
    }

    public static int getOperationTypeByDesc(String tt) {
        if (tt == null) {
            return 0;
        }

        for (int i = 0; i < operationTypeDesc.length; i++) {
            if (operationTypeDesc[i].equalsIgnoreCase(tt)) {
                return i;
            }
        }
        // If this fails, try to match using the code.
        return getOperationTypeByCode(tt);
    }

    private static int getOperationTypeByCode(String tt) {
        if (tt == null) {
            return 0;
        }

        for (int i = 0; i < operationTypeCode.length; i++) {
            if (operationTypeCode[i].equalsIgnoreCase(tt)) {
                return i;
            }
        }
        return 0;
    }

    public static String getOperationTypeDesc(int i) {
        if (i < 0 || i >= operationTypeDesc.length) {
            return operationTypeDesc[0];
        }
        return operationTypeDesc[i];
    }

    public void setOperationType(int operationType) {
        this.operationType = operationType;
    }

    public static String getGenerationTypeDesc(int i) {
        if (i < 0 || i >= generationTypeDesc.length) {
            return generationTypeDesc[0];
        }
        return generationTypeDesc[i];
    }


    private static int getGenerationTypeByCode(String tt) {
        if (tt == null) {
            return 0;
        }

        for (int i = 0; i < generationTypeCode.length; i++) {
            if (generationTypeCode[i].equalsIgnoreCase(tt)) {
                return i;
            }
        }
        return 0;
    }

    public int getGenerationType() {
        return generationType;
    }

    public static int getGenerationTypeByDesc(String tt) {
        if (tt == null) {
            return 0;
        }

        for (int i = 0; i < generationTypeDesc.length; i++) {
            if (generationTypeDesc[i].equalsIgnoreCase(tt)) {
                return i;
            }
        }
        // If this fails, try to match using the code.
        return getGenerationTypeByCode(tt);
    }

    public void setGenerationType(int generationType) {
        this.generationType = generationType;
    }

    /**
     *
     *
     * @return
     */

    public int getSplitOutputAfter() {
        return splitOutputAfter;
    }

    /**
     *
     * @param splitOutputAfter
     */
    public void setSplitOutputAfter(int splitOutputAfter) {
        this.splitOutputAfter = splitOutputAfter;
    }

    /**
     * @return Returns the outputFields.
     */
    public JsonOutputField[] getOutputFields() {
        return outputFields;
    }

    /**
     * @param outputFields The outputFields to set.
     */
    public void setOutputFields(JsonOutputField[] outputFields) {
        this.outputFields = outputFields;
    }


    public JsonOutputKeyField[] getKeyFields() {
        return keyFields;
    }

    public void setKeyFields(JsonOutputKeyField[] keyFields) {
        this.keyFields = keyFields;
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        readData(stepnode);
    }

    public void allocate(int nrfields) {
        outputFields = new JsonOutputField[nrfields];
    }

    public void allocateKey(int nrfields) {
        keyFields = new JsonOutputKeyField[nrfields];
    }

    public Object clone() {

        JsonOutputMeta retval = (JsonOutputMeta) super.clone();
        int nrOutputFields = outputFields.length;

        retval.allocate(nrOutputFields);

        for (int i = 0; i < nrOutputFields; i++) {
            retval.outputFields[i] = (JsonOutputField) outputFields[i].clone();
        }

        int nrKeyFields = keyFields.length;

        retval.allocateKey(nrKeyFields);

        for (int i = 0; i < nrKeyFields; i++) {
            retval.keyFields[i] = (JsonOutputKeyField) keyFields[i].clone();
        }

        return retval;
    }

    /**
     * @param AddToResult The Add file to result to set.
     */
    public void setAddToResult(boolean AddToResult) {
        this.addToResult = AddToResult;
    }

    private void readData(Node stepnode) throws KettleXMLException {
        try {
            outputValue = XMLHandler.getTagValue(stepnode, "outputValue");
            jsonBloc = XMLHandler.getTagValue(stepnode, "jsonBloc");
            operationType = getOperationTypeByCode(Const.NVL(XMLHandler.getTagValue(stepnode, "operation_type"), ""));
            generationType = getGenerationTypeByCode(Const.NVL(XMLHandler.getTagValue(stepnode, "generation_type"), ""));
            useArrayWithSingleInstance = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "use_arrays_with_single_instance"));
            jsonPrittified = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "json_prittified"));
            splitOutputAfter = Integer.parseInt(XMLHandler.getTagValue(stepnode, "split_output_after"));

            encoding = XMLHandler.getTagValue(stepnode, "encoding");
            addToResult = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "addToResult"));
            fileName = XMLHandler.getTagValue(stepnode, "file", "name");
            createparentfolder = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "file", "create_parent_folder"));
            extension = XMLHandler.getTagValue(stepnode, "file", "extention");
            fileAppended = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "file", "append"));
            stepNrInFilename = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "file", "split"));
            partNrInFilename = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "file", "haspartno"));
            dateInFilename = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "file", "add_date"));
            timeInFilename = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "file", "add_time"));
            doNotOpenNewFileInit = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "file", "doNotOpenNewFileInit"));
            servletOutput = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "file", "servlet_output"));

            Node keyFieldNodes = XMLHandler.getSubNode(stepnode, "key_fields");
            int nrKeyFields = XMLHandler.countNodes(keyFieldNodes, "key_field");

            allocateKey(nrKeyFields);

            for (int i = 0; i < nrKeyFields; i++) {
                Node fnode = XMLHandler.getSubNodeByNr(keyFieldNodes, "key_field", i);

                keyFields[i] = new JsonOutputKeyField();
                keyFields[i].setFieldName(XMLHandler.getTagValue(fnode, "key_field_name"));
            }

            Node fields = XMLHandler.getSubNode(stepnode, "fields");
            int nrfields = XMLHandler.countNodes(fields, "field");

            allocate(nrfields);

            for (int i = 0; i < nrfields; i++) {
                Node fnode = XMLHandler.getSubNodeByNr(fields, "field", i);

                outputFields[i] = new JsonOutputField();
                outputFields[i].setFieldName(XMLHandler.getTagValue(fnode, "name"));
                outputFields[i].setElementName(XMLHandler.getTagValue(fnode, "element"));
                outputFields[i].setJSONFragment(!"N".equalsIgnoreCase(XMLHandler.getTagValue(fnode, "json_fragment")));
                outputFields[i].setRemoveIfBlank(!"N".equalsIgnoreCase(XMLHandler.getTagValue(fnode, "remove_if_blank")));
            }

            jsonPageStartsAtFieldname = XMLHandler.getTagValue(stepnode, "additional_fields", "json_page_starts_at_field");
            jsonPageEndsAtFieldname = XMLHandler.getTagValue(stepnode, "additional_fields", "json_page_ends_at_field");
            jsonSizeFieldname = XMLHandler.getTagValue(stepnode, "additional_fields", "json_size_field");

        } catch (Exception e) {
            throw new KettleXMLException("Unable to load step info from XML", e);
        }
    }

    public void setDefault() {

        encoding = Const.XML_ENCODING;
        outputValue = "outputValue";
        jsonBloc = "result";
        splitOutputAfter = 0;
        operationType = OPERATION_TYPE_WRITE_TO_FILE;
        generationType = GENERATON_TYPE_FLAT;
        extension = "js";

        int nrfields = 0;

        allocate(nrfields);

        for (int i = 0; i < nrfields; i++) {
            outputFields[i] = new JsonOutputField();
            outputFields[i].setFieldName("field" + i);
            outputFields[i].setElementName("field" + i);
        }

        int nrKeyFields = 0;

        allocateKey(nrKeyFields);

        for (int i = 0; i < nrKeyFields; i++) {
            keyFields[i] = new JsonOutputKeyField();
            keyFields[i].setFieldName("key_field" + i);
        }
    }

    public void getFields(RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

        if (getOperationType() != OPERATION_TYPE_WRITE_TO_FILE) {
            RowMetaInterface rowMeta = row.clone();
            row.clear();

            JsonOutputKeyField[] keyFields = this.getKeyFields();
            for (int i=0; i<this.getKeyFields().length; i++) {
                ValueMetaInterface vmi = rowMeta.getValueMeta(rowMeta.indexOfValue(keyFields[i].getFieldName()));
                row.addValueMeta(i, vmi);
            }


            // This is JSON block's column
            row.addValueMeta(this.getKeyFields().length, new ValueMetaString(this.getOutputValue()));

            int fieldLength = this.getKeyFields().length + 1;
            if (this.jsonSizeFieldname != null && this.jsonSizeFieldname.length()>0) {
                row.addValueMeta(fieldLength, new ValueMetaInteger(this.jsonSizeFieldname));
                fieldLength++;
            }
            if (this.jsonPageStartsAtFieldname != null && this.jsonPageStartsAtFieldname.length()>0) {
                row.addValueMeta(fieldLength, new ValueMetaInteger(this.jsonPageStartsAtFieldname));
                fieldLength++;
            }
            if (this.jsonPageEndsAtFieldname != null && this.jsonPageEndsAtFieldname.length()>0) {
                row.addValueMeta(fieldLength, new ValueMetaInteger(this.jsonPageEndsAtFieldname));
                fieldLength++;
            }
        }
    }

    public String getXML() {
        StringBuffer retval = new StringBuffer(500);

        retval.append("    ").append(XMLHandler.addTagValue("outputValue", outputValue));
        retval.append("    ").append(XMLHandler.addTagValue("jsonBloc", jsonBloc));
        retval.append("    ").append(XMLHandler.addTagValue("operation_type", getOperationTypeCode(operationType)));
        retval.append("    ").append(XMLHandler.addTagValue("generation_type", getGenerationTypeCode(generationType)));
        retval.append("    ").append(XMLHandler.addTagValue("use_arrays_with_single_instance", useArrayWithSingleInstance));
        retval.append("    ").append(XMLHandler.addTagValue("json_prittified", jsonPrittified));
        retval.append("    ").append(XMLHandler.addTagValue("split_output_after", Integer.toString(splitOutputAfter)));
        retval.append("    ").append(XMLHandler.addTagValue("encoding", encoding));
        retval.append("    ").append(XMLHandler.addTagValue("addtoresult", addToResult));
        retval.append("    <file>" + Const.CR);
        retval.append("      ").append(XMLHandler.addTagValue("name", fileName));
        retval.append("      ").append(XMLHandler.addTagValue("extention", extension));
        retval.append("      ").append(XMLHandler.addTagValue("append", fileAppended));
        retval.append("      ").append(XMLHandler.addTagValue("split", stepNrInFilename));
        retval.append("      ").append(XMLHandler.addTagValue("haspartno", partNrInFilename));
        retval.append("      ").append(XMLHandler.addTagValue("add_date", dateInFilename));
        retval.append("      ").append(XMLHandler.addTagValue("add_time", timeInFilename));
        retval.append("      ").append(XMLHandler.addTagValue("create_parent_folder", createparentfolder));
        retval.append("      ").append(XMLHandler.addTagValue("doNotOpenNewFileInit", doNotOpenNewFileInit));
        retval.append("      ").append(XMLHandler.addTagValue("servlet_output", servletOutput));
        retval.append("      </file>" + Const.CR);
        retval.append("     <additional_fields>" + Const.CR);
        retval.append("      ").append(XMLHandler.addTagValue("json_page_starts_at_field", jsonPageStartsAtFieldname));
        retval.append("      ").append(XMLHandler.addTagValue("json_page_ends_at_field", jsonPageEndsAtFieldname));
        retval.append("      ").append(XMLHandler.addTagValue("json_size_field", jsonSizeFieldname));
        retval.append("      </additional_fields>" + Const.CR);

        retval.append("    <key_fields>").append(Const.CR);
        for (int i = 0; i < keyFields.length; i++) {
            JsonOutputKeyField keyField = keyFields[i];

            if (keyField.getFieldName() != null && keyField.getFieldName().length() != 0) {
                retval.append("      <key_field>").append(Const.CR);
                retval.append("        ").append(XMLHandler.addTagValue("key_field_name", keyField.getFieldName()));
                retval.append("    </key_field>" + Const.CR);
            }
        }
        retval.append("    </key_fields>").append(Const.CR);

        retval.append("    <fields>").append(Const.CR);
        for (int i = 0; i < outputFields.length; i++) {
            JsonOutputField field = outputFields[i];

            if (field.getFieldName() != null && field.getFieldName().length() != 0) {
                retval.append("      <field>").append(Const.CR);
                retval.append("        ").append(XMLHandler.addTagValue("name", field.getFieldName()));
                retval.append("        ").append(XMLHandler.addTagValue("element", field.getElementName()));
                retval.append("        ").append(XMLHandler.addTagValue("json_fragment", field.isJSONFragment()));
                retval.append("        ").append(XMLHandler.addTagValue("remove_if_blank", field.isRemoveIfBlank()));
                retval.append("    </field>" + Const.CR);
            }
        }
        retval.append("    </fields>").append(Const.CR);
        return retval.toString();
    }

    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
            throws KettleException {
        try {
            outputValue = rep.getStepAttributeString(id_step, "outputValue");
            jsonBloc = rep.getStepAttributeString(id_step, "jsonBloc");

            operationType = getOperationTypeByCode(Const.NVL(rep.getStepAttributeString(id_step, "operation_type"), ""));
            generationType = getGenerationTypeByCode(Const.NVL(rep.getStepAttributeString(id_step, "generation_type"), ""));
            useArrayWithSingleInstance = rep.getStepAttributeBoolean(id_step, "use_arrays_with_single_instance");
            jsonPrittified = rep.getStepAttributeBoolean(id_step, "json_prittified");
            splitOutputAfter = Integer.parseInt(rep.getStepAttributeString(id_step, "split_output_after"));
            encoding = rep.getStepAttributeString(id_step, "encoding");
            addToResult = rep.getStepAttributeBoolean(id_step, "addtoresult");

            fileName = rep.getStepAttributeString(id_step, "file_name");
            extension = rep.getStepAttributeString(id_step, "file_extention");
            fileAppended = rep.getStepAttributeBoolean(id_step, "file_append");
            stepNrInFilename = rep.getStepAttributeBoolean(id_step, "file_add_stepnr");
            partNrInFilename = rep.getStepAttributeBoolean(id_step, "file_add_partnr");
            dateInFilename = rep.getStepAttributeBoolean(id_step, "file_add_date");
            timeInFilename = rep.getStepAttributeBoolean(id_step, "file_add_time");
            createparentfolder = rep.getStepAttributeBoolean(id_step, "create_parent_folder");
            doNotOpenNewFileInit = rep.getStepAttributeBoolean(id_step, "doNotOpenNewFileInit");
            servletOutput = rep.getStepAttributeBoolean(id_step, "file_servlet_output");
            jsonSizeFieldname = rep.getStepAttributeString(id_step, "json_size_field");
            jsonPageStartsAtFieldname = rep.getStepAttributeString(id_step, "json_page_starts_at_field");
            jsonPageEndsAtFieldname = rep.getStepAttributeString(id_step, "json_page_ends_at_field");

            int nrKeyFields = rep.countNrStepAttributes(id_step, "key_field_name");

            allocateKey(nrKeyFields);

            for (int i = 0; i < nrKeyFields; i++) {
                keyFields[i] = new JsonOutputKeyField();

                outputFields[i].setFieldName(rep.getStepAttributeString(id_step, i, "key_field_name"));
            }

            int nrfields = rep.countNrStepAttributes(id_step, "field_name");

            allocate(nrfields);

            for (int i = 0; i < nrfields; i++) {
                outputFields[i] = new JsonOutputField();

                outputFields[i].setFieldName(rep.getStepAttributeString(id_step, i, "field_name"));
                outputFields[i].setElementName(rep.getStepAttributeString(id_step, i, "field_element"));
                outputFields[i].setJSONFragment(!"N".equalsIgnoreCase(rep.getStepAttributeString(id_step, i, "json_fragment")));
                outputFields[i].setRemoveIfBlank(!"N".equalsIgnoreCase(rep.getStepAttributeString(id_step, i, "remove_if_blank")));
            }
        } catch (Exception e) {
            throw new KettleException("Unexpected error reading step information from the repository", e);
        }
    }

    private static String getOperationTypeCode(int i) {
        if (i < 0 || i >= operationTypeCode.length) {
            return operationTypeCode[0];
        }
        return operationTypeCode[i];
    }

    private static String getGenerationTypeCode(int i) {
        if (i < 0 || i >= generationTypeCode.length) {
            return generationTypeCode[0];
        }
        return generationTypeCode[i];
    }

    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
            throws KettleException {
        try {
            rep.saveStepAttribute(id_transformation, id_step, "outputValue", outputValue);
            rep.saveStepAttribute(id_transformation, id_step, "jsonBloc", jsonBloc);

            rep.saveStepAttribute(id_transformation, id_step, "operation_type", getOperationTypeCode(operationType));
            rep.saveStepAttribute(id_transformation, id_step, "use_arrays_with_single_instance", useArrayWithSingleInstance);
            rep.saveStepAttribute(id_transformation, id_step, "generation_type", getGenerationTypeCode(operationType));
            rep.saveStepAttribute(id_transformation, id_step, "json_prittified", jsonPrittified);
            rep.saveStepAttribute(id_transformation, id_step, "split_output_after", splitOutputAfter);
            rep.saveStepAttribute(id_transformation, id_step, "encoding", encoding);
            rep.saveStepAttribute(id_transformation, id_step, "addtoresult", addToResult);

            rep.saveStepAttribute(id_transformation, id_step, "file_name", fileName);
            rep.saveStepAttribute(id_transformation, id_step, "file_extention", extension);
            rep.saveStepAttribute(id_transformation, id_step, "file_append", fileAppended);
            rep.saveStepAttribute(id_transformation, id_step, "file_add_stepnr", stepNrInFilename);
            rep.saveStepAttribute(id_transformation, id_step, "file_add_partnr", partNrInFilename);
            rep.saveStepAttribute(id_transformation, id_step, "file_add_date", dateInFilename);
            rep.saveStepAttribute(id_transformation, id_step, "file_add_time", timeInFilename);
            rep.saveStepAttribute(id_transformation, id_step, "create_parent_folder", createparentfolder);
            rep.saveStepAttribute(id_transformation, id_step, "doNotOpenNewFileInit", doNotOpenNewFileInit);
            rep.saveStepAttribute(id_transformation, id_step, "file_servlet_output", servletOutput);
            rep.saveStepAttribute(id_transformation, id_step, "json_size_field", jsonSizeFieldname);
            rep.saveStepAttribute(id_transformation, id_step, "json_page_starts_at_field", jsonPageStartsAtFieldname);
            rep.saveStepAttribute(id_transformation, id_step, "json_page_ends_at_field", jsonPageEndsAtFieldname);

            for (int i = 0; i < keyFields.length; i++) {
                JsonOutputKeyField keyField = keyFields[i];

                rep.saveStepAttribute(id_transformation, id_step, i, "key_field_name", keyField.getFieldName());
            }

            for (int i = 0; i < outputFields.length; i++) {
                JsonOutputField field = outputFields[i];

                rep.saveStepAttribute(id_transformation, id_step, i, "field_name", field.getFieldName());
                rep.saveStepAttribute(id_transformation, id_step, i, "field_element", field.getElementName());
                rep.saveStepAttribute(id_transformation, id_step, i, "json_fragment", field.isJSONFragment());
                rep.saveStepAttribute(id_transformation, id_step, i, "remove_if_blank", field.isRemoveIfBlank());
            }
        } catch (Exception e) {
            throw new KettleException("Unable to save step information to the repository for id_step=" + id_step, e);
        }
    }

    public String[] getFiles(String fileName) {
        int copies = 1;
        int splits = 1;
        int parts = 1;

        if (stepNrInFilename) {
            copies = 3;
        }

        if (partNrInFilename) {
            parts = 3;
        }

        int nr = copies * parts * splits;
        if (nr > 1) {
            nr++;
        }

        String[] retval = new String[nr];

        int i = 0;
        for (int copy = 0; copy < copies; copy++) {
            for (int part = 0; part < parts; part++) {
                for (int split = 0; split < splits; split++) {
                    retval[i] = buildFilename(fileName, copy, split);
                    i++;
                }
            }
        }
        if (i < nr) {
            retval[i] = "...";
        }

        return retval;
    }

    public String buildFilename(String fileName, int stepnr, int splitnr) {
        SimpleDateFormat daf = new SimpleDateFormat();

        // Replace possible environment variables...
        String retval = fileName;

        Date now = new Date();

        if (dateInFilename) {
            daf.applyPattern("yyyMMdd");
            String d = daf.format(now);
            retval += "_" + d;
        }
        if (timeInFilename) {
            daf.applyPattern("HHmmss.SSS");
            String t = daf.format(now);
            retval += "_" + t;
        }
        if (stepNrInFilename) {
            retval += "_" + stepnr;
        }

        if (extension != null && extension.length() != 0) {
            retval += "." + extension;
        }

        return retval;
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
                      IMetaStore metaStore) {

        CheckResult cr;
        if (getOperationType() != JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE) {
            // We need to have output field name
            if (Const.isEmpty(transMeta.environmentSubstitute(getOutputValue()))) {
                cr =
                        new CheckResult(CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
                                "JsonOutput.Error.MissingOutputFieldName"), stepMeta);
                remarks.add(cr);
            }
        }
        if (Const.isEmpty(transMeta.environmentSubstitute(getFileName()))) {
            cr =
                    new CheckResult(CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
                            "JsonOutput.Error.MissingTargetFilename"), stepMeta);
            remarks.add(cr);
        }
        // Check output fields
        if (prev != null && prev.size() > 0) {
            cr =
                    new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                            "JsonOutputMeta.CheckResult.FieldsReceived", "" + prev.size()), stepMeta);
            remarks.add(cr);

            String error_message = "";
            boolean error_found = false;

            // Starting from selected fields in ...
            for (int i = 0; i < outputFields.length; i++) {
                int idx = prev.indexOfValue(outputFields[i].getFieldName());
                if (idx < 0) {
                    error_message += "\t\t" + outputFields[i].getFieldName() + Const.CR;
                    error_found = true;
                }
            }
            if (error_found) {
                error_message = BaseMessages.getString(PKG, "JsonOutputMeta.CheckResult.FieldsNotFound", error_message);
                cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta);
                remarks.add(cr);
            } else {
                cr =
                        new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                                "JsonOutputMeta.CheckResult.AllFieldsFound"), stepMeta);
                remarks.add(cr);
            }
        }

        // See if we have input streams leading to this step!
        if (input.length > 0) {
            cr =
                    new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                            "JsonOutputMeta.CheckResult.ExpectedInputOk"), stepMeta);
            remarks.add(cr);
        } else {
            cr =
                    new CheckResult(CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
                            "JsonOutputMeta.CheckResult.ExpectedInputError"), stepMeta);
            remarks.add(cr);
        }

        cr =
                new CheckResult(CheckResult.TYPE_RESULT_COMMENT, BaseMessages.getString(PKG,
                        "JsonOutputMeta.CheckResult.FilesNotChecked"), stepMeta);
        remarks.add(cr);
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
                                 Trans trans) {
        return new JsonOutput(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    public StepDataInterface getStepData() {
        return new JsonOutputData();
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /**
     * @return Returns the jsonBloc.
     */
    public String getJsonBloc() {
        return jsonBloc;
    }

    /**
     * @param jsonBloc The root node to set.
     */
    public void setJsonBloc(String jsonBloc) {
        this.jsonBloc = jsonBloc;
    }

    public String getOutputValue() {
        return outputValue;
    }

    public void setOutputValue(String outputValue) {
        this.outputValue = outputValue;
    }

    public boolean isServletOutput() {
        return servletOutput;
    }

    public void setServletOutput(boolean servletOutput) {
        this.servletOutput = servletOutput;
    }

    public boolean isUseArrayWithSingleInstance() {
        return useArrayWithSingleInstance;
    }

    public void setUseArrayWithSingleInstance(boolean useArrayWithSingleInstance) {
        this.useArrayWithSingleInstance = useArrayWithSingleInstance;
    }

    public boolean isJsonPrittified() {
        return jsonPrittified;
    }

    public void setJsonPrittified(boolean jsonPrittified) {
        this.jsonPrittified = jsonPrittified;
    }
}
