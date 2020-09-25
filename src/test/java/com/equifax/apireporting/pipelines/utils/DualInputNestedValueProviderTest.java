package com.equifax.apireporting.pipelines.utils;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class DualInputNestedValueProviderTest {
    @Rule
    public ExpectedException expectedException = org.junit.rules.ExpectedException.none();

    /** A test interface. */
    public interface TestOptions extends PipelineOptions {
        @Default.String("fake")
        ValueProvider<String> getFake();

        void setFake(ValueProvider<String> fake);

        ValueProvider<Integer> getFakeNumber();

        void setFakeNumber(ValueProvider<Integer> fakeNumber);
    }

    @Test
    public void testNestedValueProviderStatic() throws Exception {
        ValueProvider<String> xvp = ValueProvider.StaticValueProvider.of("fake");
        ValueProvider<Integer> yvp = ValueProvider.StaticValueProvider.of(1);
        ValueProvider<String> zvp =
                DualInputNestedValueProvider.of(
                        xvp,
                        yvp,
                        new SerializableFunction<DualInputNestedValueProvider.TranslatorInput<String, Integer>, String>() {
                            @Override
                            public String apply(DualInputNestedValueProvider.TranslatorInput<String, Integer> input) {
                                return input.getX() + (input.getY() + 1);
                            }
                        });
        assertTrue(zvp.isAccessible());
        assertEquals("fake2", zvp.get());
    }

    @Test
    public void testNestedValueProviderRuntime() throws Exception {
        TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
        ValueProvider<String> fake = options.getFake();
        ValueProvider<Integer> fakeNumber = options.getFakeNumber();
        ValueProvider<String> fakeFakeNumber =
                DualInputNestedValueProvider.of(
                        fake,
                        fakeNumber,
                        new SerializableFunction<DualInputNestedValueProvider.TranslatorInput<String, Integer>, String>() {
                            @Override
                            public String apply(DualInputNestedValueProvider.TranslatorInput<String, Integer> input) {
                                return input.getX() + (input.getY() + 1);
                            }
                        });
        assertFalse(fakeFakeNumber.isAccessible());
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Value only available at runtime");
        fakeFakeNumber.get();
    }
}
