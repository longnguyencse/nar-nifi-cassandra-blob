/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ifp.at.mikorn.com;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.Logger;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.*;

@Tags({"cassandra", "cql"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FileHdfsProcessor extends AbstractProcessor {

    private static Logger logger = org.apache.logging.log4j
            .LogManager.getLogger(FileHdfsProcessor.class.getName());

    private static String CASSANDRA_CONTACT_POINTS = "192.168.1.152";
    private static String KEY_SPACE = "ifp";
    private static String USER_NAME = "ifp";
    private static String PASSWORD = "test";

    private static String FILE_NAME         = "file_name";
    private static String FRAME_INDEX       = "frame_index";
    private static String UNIT_EXTENSION    = "unit_extension";

    public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor
            .Builder().name("Cassandra contact point")
            .displayName("Cassandra contact point")
            .description("Cassandra contact point, default port 9042")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access the Cassandra cluster")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access the Cassandra cluster")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the record data.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_FILENAME_QUERY = new PropertyDescriptor.Builder()
            .name("File name query")
            .description("Attribute filenam")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_FRAME_INDEX_QUERY = new PropertyDescriptor.Builder()
            .name("Frame index query")
            .description("Frame index filenam")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_UNIT_EXTENSION_QUERY = new PropertyDescriptor.Builder()
            .name("Unit extension query")
            .description("Unit extension query")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor PROP_KEY_SPACE = new PropertyDescriptor
            .Builder().name("Key space")
            .displayName("Key space")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_TABLE = new PropertyDescriptor
            .Builder().name("Table query")
            .displayName("Table query")
            .description("Table query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully executed CQL statement.")
            .build();
    // Relationships
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("CQL statement execution failed.")
            .build();
    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is transferred to this relationship if the statement cannot be executed successfully but "
                    + "attempting the operation again may succeed.")
            .build();
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(CONTACT_POINTS);
        _propertyDescriptors.add(PROP_KEY_SPACE);
        _propertyDescriptors.add(PROP_USERNAME);
        _propertyDescriptors.add(PROP_PASSWORD);
        _propertyDescriptors.add(PROP_TABLE);
        _propertyDescriptors.add(PROP_FILENAME_QUERY);
        _propertyDescriptors.add(PROP_FRAME_INDEX_QUERY);
        _propertyDescriptors.add(PROP_UNIT_EXTENSION_QUERY);
        _propertyDescriptors.add(CHARSET);
        descriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        // get properties

        session.read(flowFile, in -> {
            try {
                // get attribute
//                String fileName = flowFile.getAttribute(FILE_NAME);
                final String attrFileName = context.getProperty(PROP_FILENAME_QUERY).getValue();
                String fileName = flowFile.getAttribute(attrFileName);

//                String frameIndex = flowFile.getAttribute(FRAME_INDEX);
                final String attrFrameIndex = context.getProperty(PROP_FRAME_INDEX_QUERY).getValue();
                String frameIndex = flowFile.getAttribute(attrFrameIndex);

//                String unitExtension = flowFile.getAttribute(UNIT_EXTENSION);
                final String attrUnitExtension = context.getProperty(PROP_UNIT_EXTENSION_QUERY).getValue();
                String unitExtension = flowFile.getAttribute(attrUnitExtension);

                logger.info(String.format("filename: %s, frame index : %s, unitExtension: %s",
                        fileName, frameIndex, unitExtension));

                final String cassandraContactPoint = context.getProperty(CONTACT_POINTS).getValue();
                final String userName       = context.getProperty(PROP_USERNAME).getValue();
                final String password       = context.getProperty(PROP_PASSWORD).getValue();
                final String table          = context.getProperty(PROP_TABLE).getValue();
                final String keyspace       = context.getProperty(PROP_KEY_SPACE).getValue();

                Cluster cluster = Cluster.builder()
                        .addContactPoint(cassandraContactPoint)
                        .withCredentials(userName, password)
                        .build();
                //session
                Session sessionCas = cluster.connect(keyspace);

                byte[] bytes =  IOUtils.toByteArray(in);;

//                Insert insert = QueryBuilder.insertInto(keyspace, "org_bin_data")
                Insert insert = QueryBuilder.insertInto(keyspace, table)
                        .value("file_name",fileName)
                        .value("frame_index", NumberUtils.toInt(frameIndex, 0))
                        .value("created_time", System.currentTimeMillis())
                        //.value("last_modified_time", "")
                        .value("status", "ANNOTATING")
                        .value("unit_extension", unitExtension)
                        .value("raw_data", ByteBuffer.wrap(bytes));

                logger.info(insert.toString());
                ResultSet result = sessionCas.execute(insert.toString());
                // save in db
                if (result.wasApplied()) {
                    session.transfer(flowFile, REL_SUCCESS);
                }
                if (sessionCas != null) {
                    sessionCas.close();
                }
                if (cluster != null) {
                    cluster.close();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        session.transfer(flowFile, REL_FAILURE);


    }
}
