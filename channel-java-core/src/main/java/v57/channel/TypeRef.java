package v57.channel;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

public abstract class TypeRef<T> {
  private final Type type;

  protected TypeRef() {
    var superClass = getClass().getGenericSuperclass();
    if (!(superClass instanceof ParameterizedType parameterizedType)) {
      throw new IllegalStateException("TypeRef requires a concrete generic type");
    }
    this.type = parameterizedType.getActualTypeArguments()[0];
  }

  private TypeRef(Type type) {
    this.type = Objects.requireNonNull(type, "type");
  }

  public final Type type() {
    return type;
  }

  public static <T> TypeRef<T> of(Class<T> type) {
    return new SimpleTypeRef<>(type);
  }

  public static TypeRef<Object> raw(Type type) {
    return new SimpleTypeRef<>(type);
  }

  private static final class SimpleTypeRef<T> extends TypeRef<T> {
    private SimpleTypeRef(Type type) {
      super(type);
    }
  }
}
