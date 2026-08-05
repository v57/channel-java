package v57.channel;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonWriter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

public final class JsonCodec {
  private static final DslJson<Object> JSON = new DslJson<>(
      com.dslplatform.json.runtime.Settings.withRuntime().includeServiceLoader()
  );
  private static final ThreadLocal<JsonWriter> WRITER = ThreadLocal.withInitial(JSON::newWriter);

  static final TypeRef<Void> VOID = TypeRef.of(Void.class);
  static final TypeRef<Object> DYNAMIC = TypeRef.raw(Object.class);

  private JsonCodec() {
  }

  public static String encode(Object value) {
    return encode(DYNAMIC, value);
  }

  public static String encode(TypeRef<?> type, Object value) {
    if (value == null) {
      return "null";
    }

    try {
      var writer = WRITER.get();
      writer.reset();
      if (type == null || isDynamic(type.type())) {
        JSON.serialize(writer, value);
      } else if (!JSON.serialize(writer, type.type(), value)) {
        JSON.serialize(writer, value);
      }
      return writer.toString();
    } catch (IOException error) {
      throw new ChannelException("failed to encode json", error);
    }
  }

  public static String encodeNullable(TypeRef<?> type, Object value) {
    if (value == null || isVoid(type == null ? null : type.type())) {
      return null;
    }
    return encode(type, value);
  }

  public static Object decode(String json) {
    if (json == null) {
      return null;
    }
    var bytes = json.getBytes(StandardCharsets.UTF_8);
    try {
      var reader = JSON.newReader(bytes, bytes.length);
      reader.getNextToken();
      return DslJson.deserializeObject(reader);
    } catch (IOException error) {
      throw new ChannelException("failed to decode json", error);
    }
  }

  public static <T> T decode(Class<T> type, String json) {
    return decode(TypeRef.of(type), json);
  }

  @SuppressWarnings("unchecked")
  public static <T> T decode(TypeRef<T> type, String json) {
    if (json == null || json.isBlank() || isVoid(type.type())) {
      return null;
    }
    if (isDynamic(type.type())) {
      return (T) decode(json);
    }

    var bytes = json.getBytes(StandardCharsets.UTF_8);
    try {
      return (T) JSON.deserialize(type.type(), bytes, bytes.length);
    } catch (IOException error) {
      throw new ChannelException("failed to decode typed json", error);
    }
  }

  static boolean isVoid(Type type) {
    return type == Void.class || type == Void.TYPE;
  }

  static boolean isDynamic(Type type) {
    return type == Object.class;
  }
}
