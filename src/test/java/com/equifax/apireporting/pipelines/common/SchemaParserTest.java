package com.equifax.apireporting.pipelines.common;

import com.equifax.apireporting.pipelines.commons.SchemaParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SchemaParserTest {
    @Rule
    public transient TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testSchemaParser() throws Exception {

        Map<String, Object> params = new HashMap<>();
        params.put("message", "Fake information");
        params.put("age", 41);
        String actualObj = new ObjectMapper().writeValueAsString(params);

        JSONObject expectedObj;


        File tempFile = testFolder.newFile("tempJson.json");
        FileUtils.writeStringToFile(tempFile, actualObj);
        SchemaParser schemaParser = new SchemaParser();
        expectedObj = schemaParser.parseSchema(tempFile.getPath());
        assertEquals(expectedObj.toString(), actualObj);
    }
}
