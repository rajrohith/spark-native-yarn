package broadcast.cache;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import sun.misc.Unsafe;

public class ClassManipulation {

	public static void main(String[] args) throws Exception {
		
//		ClassLoaderUtils.loadClassFromBytesAs(PCRDD.class, "org.apache.spark.rdd.ParallelCollectionRDD");
//		MyGoofyClass o = new MyGoofyClass();
//		o.print();
//		
//		byte[] classBytes = getClassContent();
//		System.out.println(classBytes.length);
//		byte[] newClassBytes = ClassRenamer.renameTo("broadcast/cache/MyGoofyClass", classBytes);
//		System.out.println(newClassBytes.length);
//		
//		getUnsafe().defineClass(null, newClassBytes, 0, newClassBytes.length, ClassManipulation.class.getClassLoader(), ClassManipulation.class.getProtectionDomain());
////		System.out.println(clazz.getName());
////		Object o = clazz.newInstance();
////		Method[] methods = o.getClass().getDeclaredMethods();
////		System.out.println(Arrays.asList(methods));
//		MyGoofyClass o = new MyGoofyClass();
//		o.print();
		
//		Thread.currentThread().setContextClassLoader(new ByteReadingClassLoader(ClassLoader.getSystemClassLoader()));
//		System.out.println(ClassLoader.getSystemClassLoader());
//		monkeypatchCl();
//		System.out.println(ClassLoader.getSystemClassLoader());
//		
//		Foo foo = new Foo();
//		long intClassAddress = normalize(getUnsafe().getInt(new Integer(0), 4L));
//		long strClassAddress = normalize(getUnsafe().getInt("", 4L));
//		getUnsafe().putAddress(intClassAddress+36, strClassAddress);
		//getUnsafe().freeMemory(addr);
		
//		getUnsafe().copyMemory(addr2, addr, 8);
		System.out.println();
	}
	
	public static long sizeOf(Class<?> clazz) {
	  long maximumOffset = 0;
	  do {
	    for (Field f : clazz.getDeclaredFields()) {
	      if (!Modifier.isStatic(f.getModifiers())) {
	        maximumOffset = Math.max(maximumOffset, getUnsafe().objectFieldOffset(f));
	      }
	    }
	  } while ((clazz = clazz.getSuperclass()) != null);
	  return maximumOffset + 8;
	}
	
	static long toAddress(Object obj) {
	    Object[] array = new Object[] {obj};
	    long baseOffset = getUnsafe().arrayBaseOffset(Object[].class);
	    return normalize(getUnsafe().getInt(array, baseOffset));
	}

	private static long normalize(int value) {
	    if(value >= 0) return value;
	    return (~0L >>> 32) & value;
	}
	
	private static void monkeypatchCl() throws Exception {
		Field sclField = ClassLoader.class.getDeclaredField("scl");
		Object base = getUnsafe().staticFieldBase(sclField);
		long offset = getUnsafe().staticFieldOffset(sclField);
		getUnsafe().putObject(base, offset, new ByteReadingClassLoader(ClassLoader.getSystemClassLoader()));
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
	
	private static byte[] getClassContent() throws Exception {
	    File f = new File("bin/broadcast/cache/MyGoofyClassSub.class");
	    System.out.println(f.exists());
	    FileInputStream input = new FileInputStream(f);
	    byte[] content = new byte[(int)f.length()];
	    input.read(content);
	    input.close();
	    return content;
	}
	
	private static final class ByteReadingClassLoader extends ClassLoader {
//		private final String name;
//		private final byte[] classBytes;
		public ByteReadingClassLoader(ClassLoader classLoader) {
			super(classLoader);
//			this.classBytes = classBytes;
//			this.name = name;
		}
		
		public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
	        return loadClass(name, false);
	    }
		
		/**
		 * 
		 */
		@Override
		protected Class<?> findClass(String className) throws ClassNotFoundException {
			if (className.endsWith("Foo")){
				return null;
				//return this.defineClass(name, this.classBytes, 0, this.classBytes.length);
			}
			else {
				return super.findClass(className);
			}
		}
	}
}
