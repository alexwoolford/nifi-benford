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
package io.woolford.processors.nifibenford;

import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

@Tags({"Benfords Law", "fraud detection"})
@CapabilityDescription("Takes an input stream of text, typically the output from a document, and uses Benford's Law to classify the document as fraud/not fraud.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class BenfordsLawProcessor extends AbstractProcessor {

    public static final PropertyDescriptor ALPHA = new PropertyDescriptor
            .Builder().name("alpha")
            .description("This is the significance level at which we reject the null hypothesis and flag the input document as suspect.")
            .required(true)
            .defaultValue("0.05")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUSPECT = new Relationship.Builder()
            .name("SUSPECT")
            .description("Suspect relationship")
            .build();

    public static final Relationship NOT_SUSPECT = new Relationship.Builder()
            .name("NOT_SUSPECT")
            .description("Not suspect relationship")
            .build();

    public static final Relationship INSUFFICIENT_SAMPLE = new Relationship.Builder()
            .name("INSUFFICIENT_SAMPLE")
            .description("Insufficient numerical values to run a Chi-squared test")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ALPHA);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUSPECT);
        relationships.add(NOT_SUSPECT);
        this.relationships = Collections.unmodifiableSet(relationships);
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

        InputStream inputStream = session.read(flowFile);
        String input = new BufferedReader(new InputStreamReader(inputStream))
                .lines().collect(Collectors.joining("\n"));

        double[] benfordsArray = getBenfordsArray();
        long[] firstDigitArray = getFirstDigitArray(input);

        ChiSquareTest chiSquareTest = new ChiSquareTest();
        Boolean suspect = chiSquareTest.chiSquareTest(benfordsArray, firstDigitArray, context.getProperty(ALPHA).asDouble());

        if (suspect){
            session.transfer(flowFile, SUSPECT);
        } else {
            session.transfer(flowFile, NOT_SUSPECT);
        }

//        session.transfer(flowFile, INSUFFICIENT_SAMPLE);

    }

    private static double[] getBenfordsArray(){
        // this is the expected distribtion of first digits for Benford's Law
        double[] benfordsArray = new double[9];
        for (int i = 1; i < 10; i++){
            double benfordValue = 100 * Math.log10(1 + 1.0/i);
            benfordsArray[i -1] = benfordValue;
        }
        return benfordsArray;
    }

    private static long[] getFirstDigitArray(String input){
        String[] inputArray = input.split("\\s+");
        long[] firstDigitArray = new long[9];
        for (String elem : inputArray){
            String firstChar = String.valueOf(elem.charAt(0));
            boolean isDigit = firstChar.matches("[1-9]{1}");
            if (isDigit){
                firstDigitArray[Integer.parseInt(firstChar) - 1] = firstDigitArray[Integer.parseInt(firstChar) - 1] + 1;
            }
        }
        return firstDigitArray;
    }

}
