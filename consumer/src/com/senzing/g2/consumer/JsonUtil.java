package com.senzing.g2.consumer;

public class JsonUtil {
    private static final JsonWriterFactory PRETTY_FACTORY
        = Json.createWriterFactory(singletonMap(PRETTY_PRINTING, true));
        
    private static final JsonWriterFactory UGLY_FACTORY
        = Json.createWriterFactory(emptyMap());
    
    public static String toJsonText(JsonValue val) {
        return toJsonText(val, true);
    }

    public static String toJsonText(JsonValue val, boolean prettyPrint) {
        JsonWriterFactory factory = (prettyPrint) ? PRETTY_FACTORY : UGLY_FACTORY;
        StringWriter sw = new StringWriter();
        JsonWriter writer = factory.createWriter(sw);
        writer.write(val);
        sw.flush();
        return sw.toString();
    }
    
    public static JsonObject parseJsonObject(String jsonText) {
        if (jsonText == null) return null;
        StringReader sr = new StringReader(jsonText);
        JsonReader jsonReader = Json.createReader(sr);
        return jsonReader.readObject();
  }

    public static JsonArray parseJsonArray(String jsonText) {
        if (jsonText == null) return null;
        StringReader sr = new StringReader(jsonText);
        JsonReader jsonReader = Json.createReader(sr);
        return jsonReader.readArray();
  }
    
    
}
