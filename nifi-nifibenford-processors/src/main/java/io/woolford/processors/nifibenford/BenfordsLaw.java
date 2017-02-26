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
import java.util.stream.LongStream;

@Tags({"Benfords Law", "fraud detection"})
@CapabilityDescription("Takes an input stream of text, typically the output from a document, and uses Benford's Law to classify the document as conforming or non-conforming.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class BenfordsLaw extends AbstractProcessor {

    public static final PropertyDescriptor ALPHA = new PropertyDescriptor
            .Builder().name("alpha")
            .description("This is the significance level at which documents will be classified as conforming or non-conforming.")
            .required(true)
            .defaultValue("0.05")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MIN_SAMPLE = new PropertyDescriptor
            .Builder().name("min-sample")
            .description("This is the minimum number of numerical values to perform a Chi-squared test.")
            .required(true)
            .defaultValue("5")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship NON_CONFORMING = new Relationship.Builder()
            .name("NON_CONFORMING")
            .description("Non-conforming relationship")
            .build();

    public static final Relationship CONFORMING = new Relationship.Builder()
            .name("CONFORMING")
            .description("Conforming relationship")
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
        descriptors.add(MIN_SAMPLE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(NON_CONFORMING);
        relationships.add(CONFORMING);
        relationships.add(INSUFFICIENT_SAMPLE);
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

        // TODO: since the values returned by Benford's array don't ever change, these could be hard-coded rather than calling a function each time.
        double[] benfordsArray = getBenfordsArray();
        long[] firstDigitArray = getFirstDigitArray(input);

        long sampleSize = LongStream.of(firstDigitArray).sum();

        ChiSquareTest chiSquareTest = new ChiSquareTest();
        Boolean suspect = chiSquareTest.chiSquareTest(benfordsArray, firstDigitArray, context.getProperty(ALPHA).asDouble());

        //TODO: don't perform the chi-squared test if the sample is too small
        if (sampleSize < context.getProperty(MIN_SAMPLE).asLong()){
            session.transfer(flowFile, INSUFFICIENT_SAMPLE);
        } else if (suspect){
            session.transfer(flowFile, NON_CONFORMING);
        } else {
            session.transfer(flowFile, CONFORMING);
        }

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
