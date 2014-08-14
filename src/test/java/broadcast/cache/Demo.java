package broadcast.cache;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import sun.misc.Unsafe;
import sun.reflect.ReflectionFactory;

public class Demo {

	private static Unsafe unsafe = getUnsafe();

	public static void main(String[] args) throws Exception {
		
	 
	}

	public static long addressOf(Object o) throws Exception {
		Object[] array = new Object[] { o };

		long baseOffset = unsafe.arrayBaseOffset(Object[].class);
		int addressSize = unsafe.addressSize();
		long objectAddress;
		switch (addressSize) {
		case 4:
			objectAddress = unsafe.getInt(array, baseOffset);
			break;
		case 8:
			objectAddress = unsafe.getLong(array, baseOffset);
			break;
		default:
			throw new Error("unsupported address size: " + addressSize);
		}

		return (objectAddress);
	}

	private static Unsafe getUnsafe() {
		// Get the Unsafe object instance
		try {
			Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			return (sun.misc.Unsafe) field.get(null);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}

	}

	

	public static long sizeOf(Object object) {
		return unsafe.getAddress(normalize(unsafe.getInt(object, 8L)) + 12L);
	}

	public static long normalize(int value) {
		if (value >= 0)
			return value;
		return (~0L >>> 64) & value;
	}

	public static class Foo implements Serializable {
		private final String value;

		public String getValue() {
			return value;
		}

		public Foo(String value) {
			this.value = value;
		}
	}
}
